package cosmo

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

// PoolManager 连接池管理器
type PoolManager struct {
	client       *mongo.Client
	originalURI  string // 保存原始连接地址
	config       PoolConfig
	isChecking   atomic.Bool
	isRecovering atomic.Bool
	failureCount atomic.Int32 // 连续失败计数，用于指数退避

	metrics *Metrics
}

// PoolConfig 连接池配置
type PoolConfig struct {
	CheckInterval      time.Duration // 健康检查间隔
	CheckTimeout       time.Duration // 检查超时时间
	MaxRetries         int           // 最大重试次数
	RetryDelay         time.Duration // 重试延迟
	RecoverTimeout     time.Duration // 恢复超时时间
	StabilizationDelay time.Duration // 系统稳定延迟
	CloseDelay         time.Duration // 关闭旧客户端延迟
	CloseTimeout       time.Duration // 关闭旧客户端超时
	QuickCheckTimeout  time.Duration // 快速健康检查超时

	// 验证相关配置
	MaxBackoffDelay      time.Duration // 最大退避延迟
	RecoveryPingTimeout  time.Duration // 恢复过程中的Ping超时
	RecoveryQueryTimeout time.Duration // 恢复过程中的查询超时
} // DefaultPoolConfig 返回默认的连接池配置
func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		CheckInterval:        30 * time.Second,
		CheckTimeout:         10 * time.Second,
		MaxRetries:           3,
		RetryDelay:           2 * time.Second,
		RecoverTimeout:       30 * time.Second,
		StabilizationDelay:   2 * time.Second,
		CloseDelay:           5 * time.Second,
		CloseTimeout:         5 * time.Second,
		QuickCheckTimeout:    2 * time.Second,
		MaxBackoffDelay:      30 * time.Second,
		RecoveryPingTimeout:  5 * time.Second,
		RecoveryQueryTimeout: 5 * time.Second,
	}
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
	// 使用默认配置作为基础
	defaultConfig := DefaultPoolConfig()

	// 合并配置：用户提供的非零值优先
	if config.CheckInterval == 0 {
		config.CheckInterval = defaultConfig.CheckInterval
	}
	if config.CheckTimeout == 0 {
		config.CheckTimeout = defaultConfig.CheckTimeout
	}
	if config.MaxRetries == 0 {
		config.MaxRetries = defaultConfig.MaxRetries
	}
	if config.RetryDelay == 0 {
		config.RetryDelay = defaultConfig.RetryDelay
	}
	if config.RecoverTimeout == 0 {
		config.RecoverTimeout = defaultConfig.RecoverTimeout
	}
	if config.StabilizationDelay == 0 {
		config.StabilizationDelay = defaultConfig.StabilizationDelay
	}
	if config.CloseDelay == 0 {
		config.CloseDelay = defaultConfig.CloseDelay
	}
	if config.CloseTimeout == 0 {
		config.CloseTimeout = defaultConfig.CloseTimeout
	}
	if config.QuickCheckTimeout == 0 {
		config.QuickCheckTimeout = defaultConfig.QuickCheckTimeout
	}
	if config.MaxBackoffDelay == 0 {
		config.MaxBackoffDelay = defaultConfig.MaxBackoffDelay
	}
	if config.RecoveryPingTimeout == 0 {
		config.RecoveryPingTimeout = defaultConfig.RecoveryPingTimeout
	}
	if config.RecoveryQueryTimeout == 0 {
		config.RecoveryQueryTimeout = defaultConfig.RecoveryQueryTimeout
	}

	// 使用NewClient创建客户端
	client, err := NewClient(uri)
	if err != nil {
		panic(fmt.Sprintf("创建MongoDB客户端失败: %v", err))
	}

	return &PoolManager{
		client:      client,
		originalURI: uri, // 保存原始连接地址
		config:      config,

		metrics: &Metrics{},
	}
}

// Start 启动健康检查
func (m *PoolManager) Start() {
	scc.CGO(m.healthCheckLoop)
	logger.Debug("连接池健康检查已启动")
}

// ----------------------------------------------------------------------------
// 健康检查模块
// ----------------------------------------------------------------------------
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

	// 创建带超时的上下文
	ctx, cancel := context.WithTimeout(context.Background(), m.config.CheckTimeout)
	defer cancel()

	status := m.performHealthCheck(ctx)

	if !status.IsHealthy {
		m.metrics.FailedChecks.Add(1)
		m.metrics.LastFailureTime.Store(time.Now())
		logger.Alert("健康检查失败: %v", status.Error)
		// 增加失败计数
		m.failureCount.Add(1)
		// 尝试自动恢复
		scc.GO(func() {
			failures := m.failureCount.Load()
			logger.Error("健康检查失败，第%d次，立即尝试恢复", failures)
			m.tryRecover()
		})
	} else {
		// 健康检查通过，重置失败计数
		m.failureCount.Store(0)
		logger.Trace("健康检查通过，延迟: %v", status.Latency)
	}
}

// performHealthCheck 执行健康检查
func (m *PoolManager) performHealthCheck(ctx context.Context) HealthStatus {
	start := time.Now()

	// 1. 基础 Ping 测试
	err := m.client.Ping(ctx, nil)
	latency := time.Since(start)

	var status HealthStatus
	if err != nil {
		status = HealthStatus{
			IsHealthy: false,
			Latency:   latency,
			Error:     fmt.Errorf("健康检查ping失败: %w", err),
			Timestamp: time.Now(),
		}
		return status
	}

	// 2. 执行简单查询测试
	testStart := time.Now()
	db := m.client.Database("admin")
	var result bson.M
	err = db.RunCommand(ctx, bson.D{{Key: "ping", Value: 1}}).Decode(&result)
	testLatency := time.Since(testStart)

	if err != nil {
		status = HealthStatus{
			IsHealthy: false,
			Latency:   latency + testLatency,
			Error:     fmt.Errorf("健康检查查询测试失败: %w", err),
			Timestamp: time.Now(),
		}
		return status
	}

	// 3. 检查服务器状态（深度检查）
	serverStatusStart := time.Now()
	var serverStatus bson.M
	err = db.RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&serverStatus)
	serverStatusLatency := time.Since(serverStatusStart)

	if err != nil {
		// 服务器状态检查失败可能不是致命问题，记录警告但不标记为不健康
		logger.Debug("服务器状态检查失败: %v", err)
	} else {
		// 可以在这里添加对服务器状态的进一步检查，如连接数、队列长度等
		logger.Trace("服务器状态: %v", serverStatus)
	}

	status = HealthStatus{
		IsHealthy: true,
		Latency:   latency + testLatency + serverStatusLatency,
		Error:     nil,
		Timestamp: time.Now(),
	}

	return status
}

// CheckNow 立即执行健康检查
func (m *PoolManager) CheckNow() HealthStatus {
	ctx, cancel := context.WithTimeout(context.Background(), m.config.CheckTimeout)
	defer cancel()
	return m.performHealthCheck(ctx)
}

// IsHealthy 检查当前是否健康
func (m *PoolManager) IsHealthy() bool {
	// 直接执行快速检查
	ctx, cancel := context.WithTimeout(context.Background(), m.config.QuickCheckTimeout)
	defer cancel()

	err := m.client.Ping(ctx, nil)
	return err == nil
}

// ----------------------------------------------------------------------------
// 连接恢复模块
// ----------------------------------------------------------------------------
// tryRecover 尝试恢复连接
func (m *PoolManager) tryRecover() {
	if m.isRecovering.Swap(true) {
		return // 恢复已在进行中
	}
	defer m.isRecovering.Store(false)

	m.metrics.RecoveryAttempts.Add(1)

	logger.Debug("开始连接恢复...")

	// 创建默认超时上下文
	ctx, cancel := context.WithTimeout(context.Background(), m.config.RecoverTimeout)
	defer cancel()

	// 1. 保存旧连接引用
	oldClient := m.client

	// 2. 条件性等待稳定延迟
	// 如果是第一次失败，立即尝试恢复；连续失败时才应用稳定延迟
	failures := m.failureCount.Load()
	if failures > 1 {
		logger.Debug("连续失败%次，应用稳定延迟%v", failures, m.config.StabilizationDelay)
		time.Sleep(m.config.StabilizationDelay)
	}

	// 3. 使用配置的重试机制创建新客户端
	var newClient *mongo.Client
	var err error

	maxRetries := m.config.MaxRetries
	retryDelay := m.config.RetryDelay

	for attempt := 0; attempt <= maxRetries; attempt++ {
		// 检查上下文是否已取消
		if ctx.Err() != nil {
			logger.Error("恢复上下文已取消: %v", ctx.Err())
			return
		}

		if attempt > 0 {
			logger.Debug("连接恢复重试 (%d/%d)...", attempt, maxRetries)
			// 等待重试延迟，使用指数退避
			backoffDelay := time.Duration(math.Pow(2, float64(attempt-1))) * retryDelay
			if backoffDelay > m.config.MaxBackoffDelay {
				backoffDelay = m.config.MaxBackoffDelay // 最大退避延迟
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
		pingCtx, pingCancel := context.WithTimeout(ctx, m.config.RecoveryPingTimeout)
		err = newClient.Ping(pingCtx, nil)
		pingCancel()
		if err != nil {
			logger.Error("Ping验证失败 (尝试 %d/%d): %v", attempt+1, maxRetries+1, err)
			// 关闭失败的新客户端
			closeCtx, closeCancel := context.WithTimeout(context.Background(), m.config.CloseTimeout)
			newClient.Disconnect(closeCtx)
			closeCancel()
			continue
		}

		// 4.2 执行简单查询测试
		queryCtx, queryCancel := context.WithTimeout(ctx, m.config.RecoveryQueryTimeout)
		db := newClient.Database("admin")
		var result bson.M
		err = db.RunCommand(queryCtx, bson.D{{Key: "ping", Value: 1}}).Decode(&result)
		queryCancel()
		if err != nil {
			logger.Error("查询验证失败 (尝试 %d/%d): %v", attempt+1, maxRetries+1, err)
			// 关闭失败的新客户端
			closeCtx, closeCancel := context.WithTimeout(context.Background(), m.config.CloseTimeout)
			newClient.Disconnect(closeCtx)
			closeCancel()
			continue
		}

		// 4.3 检查连接池状态
		poolCtx, poolCancel := context.WithTimeout(ctx, m.config.RecoveryQueryTimeout)
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
		time.Sleep(m.config.CloseDelay)
		logger.Debug("开始关闭旧客户端...")

		// 使用带超时的上下文关闭旧客户端
		closeCtx, closeCancel := context.WithTimeout(context.Background(), m.config.CloseTimeout)
		defer closeCancel()

		// 关闭前再次验证旧客户端是否仍被使用（防止并发问题）
		if oldClient == m.client {
			logger.Debug("旧客户端仍在使用中，跳过关闭")
			return
		}

		if err := oldClient.Disconnect(closeCtx); err != nil {
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
// WaitForHealthy 等待连接恢复健康
func (m *PoolManager) WaitForHealthy(ctx context.Context, timeout time.Duration) bool {
	logger.Debug("等待连接恢复健康...")

	// 如果没有传入上下文，则创建默认上下文
	if ctx == nil {
		ctx = context.Background()
	}
	// 创建带超时的上下文
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	checkTicker := time.NewTicker(2 * time.Second) // 硬编码为2秒的检查间隔
	defer checkTicker.Stop()

	healthyCount := 0
	neededHealthy := 2 // 硬编码为需要2次连续健康检查通过

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

// PrepareForBulkOperation 准备批量操作
func (m *PoolManager) PrepareForBulkOperation(ctx context.Context) error {
	logger.Debug("为批量操作做准备...")

	// 1. 确保连接健康
	if !m.IsHealthy() {
		logger.Error("连接不健康，先尝试恢复...")
		if !m.WaitForHealthy(ctx, 30*time.Second) {
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

// warmupConnections 预热连接
func (m *PoolManager) warmupConnections(ctx context.Context) error {
	logger.Debug("预热数据库连接...")

	// 如果没有传入上下文，则创建默认超时上下文
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), m.config.RecoverTimeout)
		defer cancel()
	}

	// 执行几个简单的查询来预热连接
	for i := 0; i < 5; i++ { // 硬编码为5次预热查询
		db := m.client.Database("admin")
		var result bson.M
		err := db.RunCommand(ctx, bson.D{{Key: "ping", Value: 1}}).Decode(&result)

		if err != nil {
			return fmt.Errorf("预热查询 %d 失败: %w", i+1, err)
		}

		time.Sleep(100 * time.Millisecond) // 硬编码为100毫秒的查询间隔
	}

	logger.Debug("连接预热完成")
	return nil
}

// Execute 安全执行数据库操作
func (m *PoolManager) Execute(ctx context.Context, operation func(*mongo.Client) error) error {
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
	if !m.WaitForHealthy(ctx, 10*time.Second) {
		logger.Error("连接恢复失败")
		return fmt.Errorf("无法恢复数据库连接: %w", err)
	}

	// 连接恢复成功，再次尝试执行操作
	logger.Debug("连接恢复成功，重试操作...")
	return operation(m.client)
}

// 指标相关函数
// GetMetrics 获取监控指标
func (m *PoolManager) GetMetrics() *Metrics {
	return m.metrics
}
