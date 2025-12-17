package health

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/hwcer/cosgo/scc"
	"github.com/hwcer/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// Manager MongoDB连接池管理器
// 负责连接池的健康检查、自动恢复和监控指标收集
// 提供高可用的数据库连接服务，支持自动重连和故障转移
type Manager struct {
	client       *mongo.Client // MongoDB客户端实例
	originalURI  string        // 保存原始连接地址
	isStarted    atomic.Bool   // 防止重复启动健康检查
	isChecking   atomic.Bool   // 健康检查进行中标记
	isRecovering atomic.Bool   // 连接恢复进行中标记
	failureCount atomic.Int32  // 连续失败计数，用于指数退避
	metrics      *Metrics      // 监控指标
}

// Config 连接池全局配置
// 包含健康检查、重试、恢复等相关参数
var Config = struct {
	CheckInterval            time.Duration // 健康检查间隔
	CheckTimeout             time.Duration // 检查超时时间
	MaxRetries               int           // 最大重试次数
	RetryDelay               time.Duration // 重试延迟
	RecoverTimeout           time.Duration // 恢复超时时间
	StabilizationDelay       time.Duration // 系统稳定延迟
	CloseDelay               time.Duration // 关闭旧客户端延迟
	CloseTimeout             time.Duration // 关闭旧客户端超时
	QuickCheckTimeout        time.Duration // 快速健康检查超时
	WaitHealthyCheckInterval time.Duration // 等待健康检查间隔
	WaitHealthyNeededCount   int           // 连续健康检查通过次数
	WarmupQueryCount         int           // 预热查询次数
	WarmupQueryInterval      time.Duration // 预热查询间隔
	ExecuteWaitTimeout       time.Duration // 执行等待超时
	FailureThreshold         int           // 连续失败阈值，超过该值才应用稳定延迟
	BackoffBase              int           // 指数退避基数
	AttemptOffset            int           // 尝试次数偏移量

	// 验证相关配置
	MaxBackoffDelay      time.Duration // 最大退避延迟
	RecoveryPingTimeout  time.Duration // 恢复过程中的Ping超时
	RecoveryQueryTimeout time.Duration // 恢复过程中的查询超时
}{
	CheckInterval:            30 * time.Second,
	CheckTimeout:             10 * time.Second,
	MaxRetries:               3,
	RetryDelay:               2 * time.Second,
	RecoverTimeout:           30 * time.Second,
	StabilizationDelay:       2 * time.Second,
	CloseDelay:               5 * time.Second,
	CloseTimeout:             5 * time.Second,
	QuickCheckTimeout:        2 * time.Second,
	WaitHealthyCheckInterval: 2 * time.Second,
	WaitHealthyNeededCount:   2,
	WarmupQueryCount:         5,
	WarmupQueryInterval:      100 * time.Millisecond,
	ExecuteWaitTimeout:       10 * time.Second,
	FailureThreshold:         1, // 超过1次连续失败才应用稳定延迟
	BackoffBase:              2, // 指数退避基数为2
	AttemptOffset:            1, // 尝试次数偏移量为1
	MaxBackoffDelay:          30 * time.Second,
	RecoveryPingTimeout:      5 * time.Second,
	RecoveryQueryTimeout:     5 * time.Second,
}

// Metrics 连接池监控指标
// 记录健康检查、恢复尝试等关键指标
type Metrics struct {
	TotalChecks          atomic.Int64 // 总健康检查次数
	FailedChecks         atomic.Int64 // 失败的健康检查次数
	RecoveryAttempts     atomic.Int64 // 连接恢复尝试次数
	SuccessfulRecoveries atomic.Int64 // 成功恢复次数
	LastCheckTime        atomic.Value // 最后一次检查时间
	LastFailureTime      atomic.Value // 最后一次失败时间
}

// Status 健康检查结果状态
type Status struct {
	IsHealthy bool          // 连接是否健康
	Latency   time.Duration // 检查延迟
	Error     error         // 错误信息（如果有）
	Timestamp time.Time     // 检查时间戳
}

// NewStatus 创建健康状态实例
// 参数 latency: 检查延迟
// 参数 err: 错误信息（nil表示健康）
// 返回值: 健康状态实例
func NewStatus(latency time.Duration, err error) *Status {
	s := &Status{
		IsHealthy: false,
		Latency:   latency,
		Error:     err,
		Timestamp: time.Now(),
	}
	if err == nil {
		s.IsHealthy = true
	}
	return s
}

// New 创建连接池管理器实例
// 参数 uri: MongoDB连接字符串
// 返回值: 连接池管理器实例
// 注意：此方法会立即创建客户端并建立连接
func New(uri string) *Manager {
	client, err := NewClient(uri)
	if err != nil {
		panic(fmt.Sprintf("创建MongoDB客户端失败: %v", err))
	}

	return &Manager{
		client:      client,
		originalURI: uri, // 保存原始连接地址
		metrics:     &Metrics{},
	}
}

// Start 启动连接池健康检查
// 注意：该方法是幂等的，多次调用不会重复启动健康检查
func (m *Manager) Start() {
	// 检查是否已经启动，如果已经启动则直接返回
	if m.isStarted.Swap(true) {
		return
	}
	scc.CGO(m.healthCheckLoop)
	logger.Debug("数据库连接池健康检查已启动")
}

// ----------------------------------------------------------------------------
// 健康检查模块
// ----------------------------------------------------------------------------
// healthCheckLoop 健康检查循环协程
// 参数 ctx: 上下文，用于控制协程退出
func (m *Manager) healthCheckLoop(ctx context.Context) {
	ticker := time.NewTicker(Config.CheckInterval)
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
// 包括ping测试和简单查询测试
// 失败时会触发自动恢复机制
func (m *Manager) checkHealth() {
	defer func() {
		if e := recover(); e != nil {
			logger.Alert("Manager checkHealth error:%v", e)
		}
	}()
	if m.isChecking.Swap(true) {
		return // 已有检查在进行中
	}
	defer m.isChecking.Store(false)

	m.metrics.TotalChecks.Add(1)
	m.metrics.LastCheckTime.Store(time.Now())

	// 创建带超时的上下文
	ctx, cancel := context.WithTimeout(context.Background(), Config.CheckTimeout)
	defer cancel()

	status := m.performHealthCheck(ctx)

	if !status.IsHealthy {
		m.metrics.FailedChecks.Add(1)
		m.metrics.LastFailureTime.Store(time.Now())
		logger.Alert("数据库健康检查失败: %v", status.Error)
		// 增加失败计数
		m.failureCount.Add(1)
		// 尝试自动恢复
		scc.GO(func() {
			failures := m.failureCount.Load()
			logger.Error("数据库健康检查失败，第%d次，立即尝试恢复", failures)
			m.tryRecover()
		})
	} else {
		// 健康检查通过，重置失败计数
		m.failureCount.Store(0)
		logger.Debug("数据库健康检查通过，延迟: %v", status.Latency)
	}
}

// performHealthCheck 执行具体的健康检查操作
// 参数 ctx: 上下文，用于控制检查超时
// 返回值: 健康检查结果
// 检查内容包括：ping测试、简单查询测试、服务器状态检查
func (m *Manager) performHealthCheck(ctx context.Context) *Status {
	start := time.Now()

	// 1. 基础 Ping 测试
	err := m.client.Ping(ctx, nil)
	latency := time.Since(start)

	if err != nil {
		return NewStatus(latency, fmt.Errorf("健康检查ping失败: %w", err))
	}

	// 2. 执行简单查询测试
	testStart := time.Now()
	db := m.client.Database("admin")
	var result bson.M
	err = db.RunCommand(ctx, bson.D{{Key: "ping", Value: 1}}).Decode(&result)
	testLatency := time.Since(testStart)

	if err != nil {
		return NewStatus(latency+testLatency, fmt.Errorf("健康检查查询测试失败: %w", err))
	}

	// 3. 检查服务器状态（深度检查）
	serverStatusStart := time.Now()
	var serverStatus bson.M
	err = db.RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&serverStatus)
	serverStatusLatency := time.Since(serverStatusStart)

	if err != nil {
		// 服务器状态检查失败可能不是致命问题，记录警告但不标记为不健康
		logger.Debug("服务器状态检查失败: %v", err)
	}

	return NewStatus(latency+testLatency+serverStatusLatency, nil)
}

// IsHealthy 快速检查连接是否健康
// 返回值: true表示连接健康，false表示连接不健康
// 注意：此方法仅执行ping测试，不包含完整的健康检查
func (m *Manager) IsHealthy() bool {
	// 直接执行快速检查
	ctx, cancel := context.WithTimeout(context.Background(), Config.QuickCheckTimeout)
	defer cancel()

	err := m.client.Ping(ctx, nil)
	return err == nil
}

// ----------------------------------------------------------------------------
// 连接恢复模块
// ----------------------------------------------------------------------------
// tryRecover 尝试恢复数据库连接
// 当健康检查失败时自动调用
// 包含指数退避重试、新客户端创建和验证、旧客户端替换等逻辑
func (m *Manager) tryRecover() {
	if m.isRecovering.Swap(true) {
		return // 恢复已在进行中
	}
	defer m.isRecovering.Store(false)

	m.metrics.RecoveryAttempts.Add(1)

	logger.Debug("开始连接恢复...")

	// 创建默认超时上下文
	ctx, cancel := context.WithTimeout(context.Background(), Config.RecoverTimeout)
	defer cancel()

	// 1. 保存旧连接引用
	oldClient := m.client

	// 2. 条件性等待稳定延迟
	// 如果是第一次失败，立即尝试恢复；连续失败时才应用稳定延迟
	failures := m.failureCount.Load()
	if failures > int32(Config.FailureThreshold) {
		logger.Debug("连续失败%次，应用稳定延迟%v", failures, Config.StabilizationDelay)
		time.Sleep(Config.StabilizationDelay)
	}

	// 3. 使用配置的重试机制创建新客户端
	var newClient *mongo.Client
	var err error

	maxRetries := Config.MaxRetries
	retryDelay := Config.RetryDelay

	for attempt := 0; attempt <= maxRetries; attempt++ {
		// 检查上下文是否已取消
		if ctx.Err() != nil {
			logger.Error("恢复上下文已取消: %v", ctx.Err())
			return
		}

		if attempt > 0 {
			logger.Debug("连接恢复重试 (%d/%d)...", attempt, maxRetries)
			// 等待重试延迟，使用指数退避
			backoffDelay := time.Duration(math.Pow(float64(Config.BackoffBase), float64(attempt-Config.AttemptOffset))) * retryDelay
			if backoffDelay > Config.MaxBackoffDelay {
				backoffDelay = Config.MaxBackoffDelay // 最大退避延迟
			}
			logger.Debug("重试延迟: %v", backoffDelay)
			time.Sleep(backoffDelay)
		}

		// 创建新客户端
		newClient, err = NewClient(m.originalURI)
		if err != nil {
			logger.Error("创建新客户端失败 (尝试 %d/%d): %v", attempt+1, maxRetries+1, err)
			continue
		}

		// 4. 全面验证新客户端
		// 4.1 基础Ping测试
		pingCtx, pingCancel := context.WithTimeout(ctx, Config.RecoveryPingTimeout)
		err = newClient.Ping(pingCtx, nil)
		pingCancel()
		if err != nil {
			logger.Error("Ping验证失败 (尝试 %d/%d): %v", attempt+1, maxRetries+1, err)
			// 关闭失败的新客户端
			closeCtx, closeCancel := context.WithTimeout(context.Background(), Config.CloseTimeout)
			_ = newClient.Disconnect(closeCtx)
			closeCancel()
			continue
		}

		// 4.2 执行简单查询测试
		queryCtx, queryCancel := context.WithTimeout(ctx, Config.RecoveryQueryTimeout)
		db := newClient.Database("admin")
		var result bson.M
		err = db.RunCommand(queryCtx, bson.D{{Key: "ping", Value: 1}}).Decode(&result)
		queryCancel()
		if err != nil {
			logger.Error("查询验证失败 (尝试 %d/%d): %v", attempt+1, maxRetries+1, err)
			// 关闭失败的新客户端
			closeCtx, closeCancel := context.WithTimeout(context.Background(), Config.CloseTimeout)
			_ = newClient.Disconnect(closeCtx)
			closeCancel()
			continue
		}

		// 4.3 检查连接池状态
		poolCtx, poolCancel := context.WithTimeout(ctx, Config.RecoveryQueryTimeout)
		var serverStatus bson.M
		err = db.RunCommand(poolCtx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&serverStatus)
		poolCancel()
		if err != nil {
			// 连接池状态检查失败不是致命错误，仅记录警告
			logger.Debug("连接池状态检查失败 (尝试 %d/%d): %v", attempt+1, maxRetries+1, err)
		} else {
			logger.Debug("新连接服务器状态: %v", serverStatus)
		}

		// 连接成功
		logger.Debug("新客户端验证成功 (尝试 %d/%d)", attempt+1, maxRetries+1)
		break
	}

	if err != nil {
		logger.Alert("所有连接恢复尝试均失败: %v", err)
		logger.Error("连接恢复失败")
		return
	}

	// 5. 替换旧客户端
	oldClient = m.client
	m.client = newClient
	logger.Debug("客户端替换成功")

	// 6. 安全关闭旧客户端
	go func() {
		// 延迟关闭旧客户端，确保新连接已稳定使用
		time.Sleep(Config.CloseDelay)
		logger.Debug("开始关闭旧客户端...")

		// 使用带超时的上下文关闭旧客户端
		closeCtx, closeCancel := context.WithTimeout(context.Background(), Config.CloseTimeout)
		defer closeCancel()

		// 关闭前再次验证旧客户端是否仍被使用（防止并发问题）
		if oldClient == m.client {
			logger.Debug("旧客户端仍在使用中，跳过关闭")
			return
		}

		if err = oldClient.Disconnect(closeCtx); err != nil {
			logger.Error("关闭旧客户端时出错: %v", err)
		} else {
			logger.Debug("旧客户端已成功关闭")
		}
	}()

	// 7. 记录恢复成功
	m.metrics.SuccessfulRecoveries.Add(1)
	m.failureCount.Store(0) // 重置失败计数
	logger.Debug("连接恢复成功")
}

// ----------------------------------------------------------------------------
// 辅助功能模块
// ----------------------------------------------------------------------------
// WaitForHealthy 等待连接恢复健康状态
// 参数 ctx: 上下文，用于控制操作超时
// 参数 timeout: 最大等待时间
// 返回值: true表示连接在超时前恢复健康，false表示超时
// 注意：此方法会连续执行健康检查，直到达到指定的连续通过次数
func (m *Manager) WaitForHealthy(ctx context.Context, timeout time.Duration) bool {
	logger.Debug("等待连接恢复健康...")

	// 如果没有传入上下文，则创建默认上下文
	if ctx == nil {
		ctx = context.Background()
	}
	// 创建带超时的上下文
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	checkTicker := time.NewTicker(Config.WaitHealthyCheckInterval) // 使用全局配置的检查间隔
	defer checkTicker.Stop()

	healthyCount := 0
	neededHealthy := Config.WaitHealthyNeededCount // 使用全局配置的连续健康检查通过次数

	for {
		select {
		case <-checkTicker.C:
			if m.IsHealthy() {
				healthyCount++
				logger.Debug("健康检查通过 (%d/%d)", healthyCount, neededHealthy)

				if healthyCount >= neededHealthy {
					logger.Debug("连接已恢复健康")
					return true
				}
			} else {
				healthyCount = 0
				logger.Error("健康检查失败，重置计数")
			}

		case <-timeoutCtx.Done():
			logger.Alert("等待超时，连接仍未恢复")
			return false
		}
	}
}

// PrepareForBulkOperation 为批量操作准备连接
// 参数 ctx: 上下文，用于控制操作超时
// 返回值: 准备过程中的错误
// 包括连接健康检查和预热，提高批量操作的性能和可靠性
func (m *Manager) PrepareForBulkOperation(ctx context.Context) error {
	logger.Debug("为批量操作做准备...")

	// 1. 确保连接健康
	if !m.IsHealthy() {
		logger.Error("连接不健康，先尝试恢复...")
		if !m.WaitForHealthy(ctx, Config.RecoverTimeout) {
			return fmt.Errorf("连接无法恢复健康")
		}
	}

	// 2. 预热连接
	err := m.warmupConnections(ctx)
	if err != nil {
		return fmt.Errorf("预热连接失败: %w", err)
	}

	logger.Debug("批量操作准备完成")
	return nil
}

// warmupConnections 预热数据库连接
// 参数 ctx: 上下文，用于控制操作超时
// 返回值: 预热过程中的错误
// 通过执行多次ping操作，确保连接池中的连接都处于活跃状态
func (m *Manager) warmupConnections(ctx context.Context) error {
	logger.Debug("预热数据库连接...")

	// 如果没有传入上下文，则创建默认超时上下文
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), Config.RecoverTimeout)
		defer cancel()
	}

	// 执行几个简单的查询来预热连接
	for i := 0; i < Config.WarmupQueryCount; i++ { // 使用全局配置的预热查询次数
		db := m.client.Database("admin")
		var result bson.M
		err := db.RunCommand(ctx, bson.D{{Key: "ping", Value: 1}}).Decode(&result)

		if err != nil {
			return fmt.Errorf("预热查询 %d 失败: %w", i+1, err)
		}

		time.Sleep(Config.WarmupQueryInterval) // 使用全局配置的预热查询间隔
	}

	logger.Debug("连接预热完成")
	return nil
}

// Execute 安全执行数据库操作
// 参数 ctx: 上下文，用于控制操作超时
// 参数 operation: 数据库操作函数，接收mongo.Client作为参数
// 返回值: 操作过程中的错误
// 提供连接健康检查和自动恢复机制，确保操作的可靠性
func (m *Manager) Execute(ctx context.Context, operation func(*mongo.Client) error) error {
	// 检查上下文是否已取消
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// 执行数据库操作
	err := operation(m.client)
	if err == nil {
		return nil // 操作成功，直接返回
	}

	// 检查上下文是否已取消
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// 检查连接是否健康
	if m.IsHealthy() {
		// 连接健康但操作失败，可能是业务错误，返回原始错误
		logger.Trace("连接健康但操作失败，可能是业务错误: %v", err)
		return err
	}

	// 连接不健康，尝试恢复
	logger.Error("操作失败，连接不健康，尝试恢复...")

	// 尝试恢复连接
	m.tryRecover()

	// 等待恢复完成
	if !m.WaitForHealthy(ctx, Config.ExecuteWaitTimeout) {
		logger.Error("连接恢复失败")
		return fmt.Errorf("无法恢复数据库连接: %w", err)
	}

	// 连接恢复成功，再次尝试执行操作
	logger.Debug("连接恢复成功，重试操作...")
	return operation(m.client)
}

// GetMetrics 获取连接池监控指标
// 返回值: 当前的监控指标实例
func (m *Manager) GetMetrics() *Metrics {
	return m.metrics
}
