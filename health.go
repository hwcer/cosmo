package cosmo

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/hwcer/cosgo/scc"
	"github.com/hwcer/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// PoolManager 连接池管理器
type PoolManager struct {
	client       *mongo.Client
	originalURI  string // 保存原始连接地址
	config       PoolConfig
	isChecking   atomic.Bool
	isRecovering atomic.Bool
	//shutdown     chan struct{}
	metrics *Metrics

	// 健康状态缓存
	lastHealthy   atomic.Bool   // 上次检查的健康状态
	lastCheckTime atomic.Value  // 上次检查的时间
	cacheDuration time.Duration // 缓存持续时间
}

// PoolConfig 连接池配置
type PoolConfig struct {
	CheckInterval    time.Duration // 健康检查间隔
	CheckTimeout     time.Duration // 检查超时时间
	MaxRetries       int           // 最大重试次数
	RetryDelay       time.Duration // 重试延迟
	HealthyThreshold int           // 连续健康次数阈值
}

// Metrics 监控指标
type Metrics struct {
	TotalChecks          atomic.Int64
	FailedChecks         atomic.Int64
	RecoveryAttempts     atomic.Int64
	SuccessfulRecoveries atomic.Int64
	LastCheckTime        atomic.Value // time.Time
	LastFailureTime      atomic.Value // time.Time
}

// HealthStatus 健康状态
type HealthStatus struct {
	IsHealthy bool
	Latency   time.Duration
	Error     error
	Timestamp time.Time
}

// NewPoolManager 创建连接池管理器
func NewPoolManager(uri string, config PoolConfig) *PoolManager {
	if config.CheckInterval == 0 {
		config.CheckInterval = 30 * time.Second
	}
	if config.CheckTimeout == 0 {
		config.CheckTimeout = 10 * time.Second
	}
	if config.MaxRetries == 0 {
		config.MaxRetries = 3
	}
	if config.RetryDelay == 0 {
		config.RetryDelay = 2 * time.Second
	}
	if config.HealthyThreshold == 0 {
		config.HealthyThreshold = 3
	}

	// 使用NewClient创建客户端
	client, err := NewClient(uri)
	if err != nil {
		panic(fmt.Sprintf("创建MongoDB客户端失败: %v", err))
	}

	// 设置默认缓存持续时间为10秒
	cacheDuration := 10 * time.Second

	return &PoolManager{
		client:      client,
		originalURI: uri, // 保存原始连接地址
		config:      config,
		//shutdown:     make(chan struct{}),
		metrics:       &Metrics{},
		cacheDuration: cacheDuration,
	}
}

// Start 启动健康检查
func (m *PoolManager) Start() {
	scc.CGO(m.healthCheckLoop)
	logger.Debug("连接池健康检查已启动")
}

// healthCheckLoop 健康检查循环
func (m *PoolManager) healthCheckLoop(ctx context.Context) {
	ticker := time.NewTicker(m.config.CheckInterval)
	defer ticker.Stop()

	// 启动时立即检查一次
	m.checkHealth()

	for {
		select {
		case <-ticker.C:
			m.checkHealth()
		case <-ctx.Done():
			return
		}
	}
}

// checkHealth 执行健康检查
func (m *PoolManager) checkHealth() {
	defer func() {
		if e := recover(); e != nil {
			logger.Alert("PoolManager checkHealth error:%v", e)
		}
	}()
	if m.isChecking.Swap(true) {
		return // 已有检查在进行中
	}
	defer m.isChecking.Store(false)

	m.metrics.TotalChecks.Add(1)
	m.metrics.LastCheckTime.Store(time.Now())

	status := m.performHealthCheck()

	if !status.IsHealthy {
		m.metrics.FailedChecks.Add(1)
		m.metrics.LastFailureTime.Store(time.Now())
		logger.Alert("健康检查失败: %v", status.Error)
		// 尝试自动恢复
		scc.GO(m.tryRecover)
	} else {
		logger.Trace("健康检查通过，延迟: %v", status.Latency)
	}
}

// performHealthCheck 执行健康检查
func (m *PoolManager) performHealthCheck() HealthStatus {
	ctx, cancel := context.WithTimeout(context.Background(), m.config.CheckTimeout)
	defer cancel()

	start := time.Now()

	// 1. 基础 Ping 测试
	err := m.client.Ping(ctx, nil)
	latency := time.Since(start)

	var status HealthStatus
	if err != nil {
		status = HealthStatus{
			IsHealthy: false,
			Latency:   latency,
			Error:     fmt.Errorf("ping失败: %w", err),
			Timestamp: time.Now(),
		}
	} else {
		// 2. 执行简单查询测试
		testStart := time.Now()
		db := m.client.Database("admin")
		var result bson.M
		err = db.RunCommand(ctx, bson.D{{"ping", 1}}).Decode(&result)
		testLatency := time.Since(testStart)

		if err != nil {
			status = HealthStatus{
				IsHealthy: false,
				Latency:   latency + testLatency,
				Error:     fmt.Errorf("查询测试失败: %w", err),
				Timestamp: time.Now(),
			}
		} else {
			status = HealthStatus{
				IsHealthy: true,
				Latency:   latency + testLatency,
				Error:     nil,
				Timestamp: time.Now(),
			}
		}
	}

	// 更新健康状态缓存
	m.lastHealthy.Store(status.IsHealthy)
	m.lastCheckTime.Store(status.Timestamp)

	return status
}

// tryRecover 尝试恢复连接
func (m *PoolManager) tryRecover() {
	if m.isRecovering.Swap(true) {
		return // 恢复已在进行中
	}
	defer m.isRecovering.Store(false)

	m.metrics.RecoveryAttempts.Add(1)

	fmt.Println("开始连接恢复...")

	// 尝试多种恢复策略
	recovered := false
	var recoveryErr error

	// 策略1: 清理并重连
	if !recovered {
		recovered, recoveryErr = m.reconnectWithCleanup()
		if recovered {
			fmt.Println("通过清理重连恢复成功")
		}
	}

	// 策略2: 完整重新连接
	if !recovered {
		recovered, recoveryErr = m.fullReconnect()
		if recovered {
			fmt.Println("通过完整重连恢复成功")
		}
	}

	// 策略3: 创建新客户端
	if !recovered {
		recovered, recoveryErr = m.createNewClient()
		if recovered {
			fmt.Println("通过创建新客户端恢复成功")
		}
	}

	if recovered {
		m.metrics.SuccessfulRecoveries.Add(1)
		fmt.Println("连接恢复成功")
	} else {
		fmt.Printf("所有恢复尝试都失败了，最后错误: %v\n", recoveryErr)
	}
}

// reconnectWithCleanup 清理并重连
func (m *PoolManager) reconnectWithCleanup() (bool, error) {
	fmt.Println("尝试清理并重连...")

	// 1. 断开连接
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := m.client.Disconnect(ctx)
	if err != nil {
		fmt.Printf("断开连接时出错: %v\n", err)
		// 继续尝试重连
	}

	// 2. 等待一小段时间
	time.Sleep(1 * time.Second)

	// 3. 重新连接
	err = m.client.Connect(ctx)
	if err != nil {
		return false, fmt.Errorf("重连失败: %w", err)
	}

	// 4. 验证连接
	err = m.client.Ping(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("重连后验证失败: %w", err)
	}

	return true, nil
}

// fullReconnect 完整重新连接
func (m *PoolManager) fullReconnect() (bool, error) {
	fmt.Println("尝试完整重新连接...")

	// 使用保存的原始URI
	originalURI := m.originalURI

	// 完全断开
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	m.client.Disconnect(ctx)

	// 等待更长时间
	time.Sleep(3 * time.Second)

	// 创建新配置
	clientOptions := options.Client().
		ApplyURI(originalURI).
		SetMaxPoolSize(100).
		SetMinPoolSize(10).
		SetSocketTimeout(60 * time.Second).
		SetConnectTimeout(30 * time.Second)

	// 创建新客户端
	newClient, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return false, fmt.Errorf("创建新客户端失败: %w", err)
	}

	// 验证新客户端
	err = newClient.Ping(ctx, nil)
	if err != nil {
		newClient.Disconnect(context.Background())
		return false, fmt.Errorf("新客户端验证失败: %w", err)
	}

	// 替换旧客户端
	oldClient := m.client
	m.client = newClient

	// 异步关闭旧客户端
	go func() {
		time.Sleep(5 * time.Second)
		oldCtx, oldCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer oldCancel()
		oldClient.Disconnect(oldCtx)
	}()

	return true, nil
}

// createNewClient 创建全新客户端
func (m *PoolManager) createNewClient() (bool, error) {
	fmt.Println("尝试创建全新客户端...")

	// 使用保存的原始URI创建新客户端
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	newClient, err := NewClient(m.originalURI)
	if err != nil {
		return false, fmt.Errorf("创建新客户端失败: %w", err)
	}

	// 验证新客户端
	err = newClient.Ping(ctx, nil)
	if err != nil {
		newClient.Disconnect(ctx)
		return false, fmt.Errorf("新客户端验证失败: %w", err)
	}

	// 替换旧客户端
	oldClient := m.client
	m.client = newClient

	// 异步关闭旧客户端
	go func() {
		time.Sleep(5 * time.Second)
		oldCtx, oldCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer oldCancel()
		oldClient.Disconnect(oldCtx)
	}()

	return true, nil
}

// CheckNow 立即执行健康检查
func (m *PoolManager) CheckNow() HealthStatus {
	return m.performHealthCheck()
}

// IsHealthy 检查当前是否健康
func (m *PoolManager) IsHealthy() bool {
	// 检查缓存是否有效
	if lastCheck, ok := m.lastCheckTime.Load().(time.Time); ok {
		if time.Since(lastCheck) < m.cacheDuration {
			// 缓存有效，使用缓存结果
			return m.lastHealthy.Load()
		}
	}

	// 缓存过期或无缓存，执行快速检查
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second) // 减少超时时间，提高响应速度
	defer cancel()

	err := m.client.Ping(ctx, nil)
	isHealthy := err == nil

	// 更新缓存
	m.lastHealthy.Store(isHealthy)
	m.lastCheckTime.Store(time.Now())

	return isHealthy
}

// GetMetrics 获取监控指标
func (m *PoolManager) GetMetrics() *Metrics {
	return m.metrics
}

// WaitForHealthy 等待连接恢复健康
func (m *PoolManager) WaitForHealthy(timeout time.Duration) bool {
	fmt.Println("等待连接恢复健康...")

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	checkTicker := time.NewTicker(2 * time.Second)
	defer checkTicker.Stop()

	healthyCount := 0
	neededHealthy := 2 // 需要连续2次健康检查通过

	for {
		select {
		case <-checkTicker.C:
			if m.IsHealthy() {
				healthyCount++
				fmt.Printf("健康检查通过 (%d/%d)\n", healthyCount, neededHealthy)

				if healthyCount >= neededHealthy {
					fmt.Println("连接已恢复健康")
					return true
				}
			} else {
				healthyCount = 0
				fmt.Println("健康检查失败，重置计数")
			}

		case <-ctx.Done():
			fmt.Println("等待超时，连接仍未恢复")
			return false
		}
	}
}

// PrepareForBulkOperation 准备批量操作
func (m *PoolManager) PrepareForBulkOperation() error {
	fmt.Println("为批量操作做准备...")

	// 1. 确保连接健康
	if !m.IsHealthy() {
		fmt.Println("连接不健康，先尝试恢复...")
		if !m.WaitForHealthy(30 * time.Second) {
			return fmt.Errorf("连接无法恢复健康")
		}
	}

	// 2. 预热连接
	err := m.warmUpConnections()
	if err != nil {
		return fmt.Errorf("预热连接失败: %w", err)
	}

	fmt.Println("批量操作准备完成")
	return nil
}

// warmUpConnections 预热连接
func (m *PoolManager) warmUpConnections() error {
	fmt.Println("预热数据库连接...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 执行几个简单的查询来预热连接
	for i := 0; i < 5; i++ {
		db := m.client.Database("admin")
		var result bson.M
		err := db.RunCommand(ctx, bson.D{{"ping", 1}}).Decode(&result)

		if err != nil {
			return fmt.Errorf("预热查询 %d 失败: %w", i+1, err)
		}

		time.Sleep(100 * time.Millisecond)
	}

	fmt.Println("连接预热完成")
	return nil
}

// Execute 安全执行数据库操作
func (m *PoolManager) Execute(operation func(*mongo.Client) error) error {
	// 检查连接是否健康
	if !m.IsHealthy() {
		fmt.Println("连接不健康，尝试恢复...")
		m.tryRecover()
		// 等待恢复
		if !m.WaitForHealthy(10 * time.Second) {
			return fmt.Errorf("无法恢复数据库连接")
		}
	}

	// 执行操作
	return operation(m.client)
}
