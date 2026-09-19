package health

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// newTestManager 构造一个不依赖真实 MongoDB 的 Manager:
// client 通过 mongo.Connect 惰性创建（不 Ping），指向不存在的地址使其始终处于"不健康"状态
func newTestManager(t *testing.T) *Manager {
	t.Helper()
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=200&connectTimeoutMS=200&heartbeatFrequencyMS=500"))
	if err != nil {
		t.Fatalf("构造测试 client 失败: %v", err)
	}
	t.Cleanup(func() {
		_ = client.Disconnect(context.Background())
	})
	m := &Manager{originalURI: "mongodb://127.0.0.1:1", metrics: &Metrics{}}
	m.client.Store(client)
	return m
}

// 写操作在连接不健康时不得自动重试——防止换新连接重试导致 $inc/$push 等非幂等写被重复应用
func TestExecuteWriteNeverRetried(t *testing.T) {
	m := newTestManager(t)

	calls := 0
	err := m.Execute(context.Background(), func(*mongo.Client) error {
		calls++
		return errors.New("boom")
	})
	if err == nil {
		t.Fatal("写操作失败应返回错误")
	}
	if !strings.Contains(err.Error(), "未自动重试") {
		t.Fatalf("错误信息应说明未自动重试: %v", err)
	}
	if calls != 1 {
		t.Fatalf("写操作只应执行一次,实际 %d 次", calls)
	}
}

// 读操作在连接不健康且恢复失败时只执行一次并返回错误（不无限等待）
func TestExecuteReadUnhealthyReturnsAfterTimeout(t *testing.T) {
	old := Config.ExecuteWaitTimeout
	Config.ExecuteWaitTimeout = 300 * time.Millisecond
	defer func() { Config.ExecuteWaitTimeout = old }()

	m := newTestManager(t)

	calls := 0
	err := m.ExecuteRead(context.Background(), func(*mongo.Client) error {
		calls++
		return errors.New("boom")
	})
	if err == nil {
		t.Fatal("恢复失败时读操作应返回错误")
	}
	if !strings.Contains(err.Error(), "无法恢复数据库连接") {
		t.Fatalf("错误信息应说明恢复失败: %v", err)
	}
	if calls != 1 {
		t.Fatalf("恢复失败时读操作只应执行一次,实际 %d 次", calls)
	}
}
