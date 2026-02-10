# Cosmo - MongoDB ORM 框架

Cosmo 是一个轻量级的 MongoDB ORM 框架，提供了简洁的 API 和强大的功能，帮助开发者更高效地与 MongoDB 数据库交互。

## 目录结构

```
cosmo/
├── clause/          # 查询条件构建模块
│   ├── build.go     # 查询条件构建逻辑
│   ├── filter.go    # 过滤器定义
│   ├── format.go    # 格式化工具
│   ├── funcs.go     # 函数工具
│   ├── query.go     # 查询条件定义
│   ├── query_test.go # 查询条件测试
│   └── where.go     # 条件构建
├── update/          # 更新操作构建模块
│   ├── build.go     # 更新操作构建逻辑
│   ├── selector.go  # 字段选择器
│   └── update.go    # 更新操作定义
├── utils/           # 工具函数模块
│   ├── const.go     # 常量定义
│   ├── utils.go     # 工具函数
│   └── utils_test.go # 工具函数测试
├── bulkWrite.go     # 批量写操作
├── cache.go         # 缓存功能
├── callbacks.go     # 回调函数
├── chainable.go     # 链式操作
├── client.go        # MongoDB 客户端
├── command.go       # 命令执行
├── config.go        # 配置管理
├── cosmo.go         # 框架核心
├── cosmo_test.go    # 框架测试
├── define.go        # 类型定义
├── errors.go        # 错误定义
├── finisher.go      # 操作完成器
├── go.mod           # Go 模块定义
├── go.sum           # Go 模块依赖
├── health.go        # 连接池健康检查
├── migrator.go      # 自动迁移
├── paging.go        # 分页功能
├── session.go       # 会话管理
└── statement.go     # 语句构建
```

## 核心功能

### 1. 连接池管理

Cosmo 提供了强大的连接池管理功能，包括：

- 自动健康检查
- 连接自动恢复
- 指数退避重试
- 监控指标收集

```go
// 创建数据库实例
import "github.com/hwcer/cosmo"

db := cosmo.New()

// 启动数据库连接
if err := db.Start("test_db", "mongodb://localhost:27017"); err != nil {
    panic(err)
}

// 关闭数据库连接
if err := db.Close(); err != nil {
    panic(err)
}
```

### 2. 查询条件构建

使用 `clause` 包构建复杂的查询条件：

```go
// 构建查询条件
q := clause.New()
q.Eq("name", "张三")
q.Gte("age", 18)
q.In("status", []string{"active", "pending"})

// 复杂条件
orClause := clause.New()
orClause.Lt("age", 18)
orClause.Gte("age", 65)
q.Match(clause.QueryOperationOR, orClause)
```

### 3. 更新操作构建

使用 `update` 包构建更新操作：

```go
// 构建更新操作
u := update.New()
u.Set("name", "李四")
u.Inc("age", 1)
u.Unset("oldField")

// 批量更新
u.Save(map[string]any{
    "status": "active",
    "updatedAt": time.Now(),
})
```

### 4. 模型定义

```go
// 定义模型
type User struct {
    ID        primitive.ObjectID `bson:"_id" json:"id"`
    Name      string             `bson:"name" json:"name"`
    Age       int                `bson:"age" json:"age"`
    Email     string             `bson:"email" json:"email"`
    Status    string             `bson:"status" json:"status"`
    CreatedAt time.Time          `bson:"created_at" json:"createdAt"`
    UpdatedAt time.Time          `bson:"updated_at" json:"updatedAt"`
}

// 预注册模型
config := &cosmo.Config{}
config.Register(&User{})
```

### 5. 数据库操作

#### 重要说明

1. **查询条件构建**：
   - 使用 `db.Model(&Model{})` 方法指定要操作的模型
   - 推荐使用SQL风格字符串构建查询条件：`Where("name = ? AND age > ?", "John", 18)`
   - 支持主键值查询：`Where("5f8d0d55b54764421b715620")`
   - 支持字段名和值的简洁格式：`Where("name", "John")`

2. **更新操作构建**：
   - 使用 `db.Update()` 方法更新单个文档
   - 使用 `db.Updates()` 方法更新多个文档
   - 推荐使用bson.M类型支持MongoDB原生操作符：`Update(bson.M{"$set": bson.M{"status": "inactive"}})`
   - 支持map类型自动转为$set操作：`Update(map[string]any{"name": "李四"})`
   - 支持update.Update类型实现复杂更新：`Update(update.Set("name", "王五").Inc("age", 1))`

#### 创建数据

```go
user := &User{
    Name:      "张三",
    Age:       25,
    Email:     "zhangsan@example.com",
    Status:    "active",
    CreatedAt: time.Now(),
    UpdatedAt: time.Now(),
}

if err := db.Create(user); err != nil {
    panic(err)
}
```

#### 查询数据

```go
// 查询单个文档
var user User
if err := db.First(&user, bson.M{"name": "张三"}); err != nil {
    panic(err)
}

// 查询多个文档
var users []User
if err := db.Model(&User{}).Where("status = ?", "active").Find(&users); err != nil {
    panic(err)
}

// 带分页的查询
var users []User
if err := db.Model(&User{}).Where("age >= ?", 18).Offset(0).Limit(10).Find(&users); err != nil {
    panic(err)
}
```

#### 更新数据

```go
// 更新单个文档（使用update.Update类型）
if err := db.Model(&User{}).Where("_id = ?", userID).Update(bson.M{"$set": bson.M{"status": "inactive"}}); err != nil {
    panic(err)
}

// 更新多个文档（使用批量更新）
if err := db.Model(&User{}).Where("age > ?", 65).Updates(bson.M{"status": "retired"}); err != nil {
    panic(err)
}

// 更新单个文档（直接使用map，自动转为$set操作）
if err := db.Model(&User{}).Where("_id = ?", userID).Update(bson.M{"name": "李四", "age": 26}); err != nil {
    panic(err)
}

// 使用Update结构体进行复杂更新
up := update.New()
up.Set("name", "王五")
up.Inc("age", 1)
up.Unset("oldField")
if err := db.Model(&User{}).Where("_id = ?", userID).Update(up); err != nil {
    panic(err)
}
```

#### 删除数据

```go
// 删除单个文档
if err := db.Model(&User{}).Where("_id = ?", userID).Delete(); err != nil {
    panic(err)
}

// 删除多个文档
if err := db.Model(&User{}).Where("status = ?", "inactive").Delete(); err != nil {
    panic(err)
}

// 根据结构体中的_id字段删除记录
if err := db.Delete(&User{Id: userID}); err != nil {
    panic(err)
}

// 删除多个记录（使用IN条件）
if err := db.Model(&User{}).Delete([]string{userID1, userID2, userID3}); err != nil {
    panic(err)
}
```

### 6. 批量操作

```go
// 创建批量写入操作
users := []*User{
    {Name: "张三", Age: 25}, 
    {Name: "李四", Age: 30},
}

bulk := db.BulkWrite(&User{})
for _, user := range users {
    bulk.InsertOne(user)
}

// 执行批量操作
if err := bulk.Execute(); err != nil {
    panic(err)
}
```

### 7. 缓存功能

Cosmo 提供了内置的缓存功能，可以提高查询性能：

```go
// 实现 CacheModel 接口
type User struct {
    // ... 字段定义
    UpdatedAt int64 `bson:"updated_at" json:"updatedAt"`
}

func (u *User) GetUpdate() int64 {
    return u.UpdatedAt
}

// 实现 CacheHandle 接口
type UserCacheHandle struct{}

func (h *UserCacheHandle) Reload(ts int64, cb cosmo.CacheSetter) error {
    // 从数据库加载数据
    var users []*User
    if err := db.Where(clause.Gt("updated_at", ts)).Find(&users); err != nil {
        return err
    }
    
    // 设置缓存
    for _, user := range users {
        cb(user.ID.Hex(), user)
    }
    
    return nil
}

// 使用缓存
handle := &UserCacheHandle{}
db.Cache(handle, 5*time.Minute)
```

### 8. 自动迁移

```go
// 自动创建或更新索引
db := cosmo.New()
db.Config.Register(&User{})
db.Config.Register(&Order{})

err := db.AutoMigrator(&User{}, &Order{})
if err != nil {
    panic(err)
}
```

### 9. 事务支持

```go
// 开始事务
tx := db.Begin()

// 执行操作
user := &User{Name: "张三"}
if err := tx.Create(user); err != nil {
    tx.Rollback()
    panic(err)
}

order := &Order{UserID: user.ID, Product: "Product A"}
if err := tx.Create(order); err != nil {
    tx.Rollback()
    panic(err)
}

// 提交事务
if err := tx.Commit(); err != nil {
    panic(err)
}
```

### 10. 连接池管理

```go
// 自定义连接池配置
cosmo.PoolConfig.CheckInterval = 15 * time.Second
cosmo.PoolConfig.MaxRetries = 5
cosmo.PoolConfig.RetryDelay = 1 * time.Second

// 创建连接池管理器
pool := cosmo.NewPoolManager("mongodb://localhost:27017")
pool.Start()

// 检查连接健康状态
status := pool.CheckNow()
if status.IsHealthy {
    fmt.Println("连接健康")
} else {
    fmt.Printf("连接不健康: %v\n", status.Error)
}
```

## 配置

### 连接池配置

```go
cosmo.PoolConfig = struct {
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
    FailureThreshold         int           // 连续失败阈值
    BackoffBase              int           // 指数退避基数
    AttemptOffset            int           // 尝试次数偏移量
    MaxBackoffDelay          time.Duration // 最大退避延迟
    RecoveryPingTimeout      time.Duration // 恢复过程中的Ping超时
    RecoveryQueryTimeout     time.Duration // 恢复过程中的查询超时
}{}
```

## 安装

```bash
go get github.com/hwcer/cosmo
```

## 快速开始

```go
package main

import (
    "fmt"
    "time"

    "github.com/hwcer/cosmo"
    "go.mongodb.org/mongo-driver/v2/bson"
)

// 定义模型
type User struct {
    ID        bson.ObjectID `bson:"_id" json:"id"`
    Name      string             `bson:"name" json:"name"`
    Age       int                `bson:"age" json:"age"`
    Email     string             `bson:"email" json:"email"`
    Status    string             `bson:"status" json:"status"`
    CreatedAt time.Time          `bson:"created_at" json:"createdAt"`
    UpdatedAt time.Time          `bson:"updated_at" json:"updatedAt"`
}

func main() {
    // 创建数据库实例
    db := cosmo.New()
    
    // 连接到数据库
    if err := db.Start("test_db", "mongodb://localhost:27017"); err != nil {
        panic(err)
    }
    defer db.Close()
    
    // 预注册模型
    db.Config.Register(&User{})
    
    // 自动迁移
    if err := db.AutoMigrator(&User{}); err != nil {
        panic(err)
    }
    
    // 创建数据
    user := &User{
        Name:      "张三",
        Age:       25,
        Email:     "zhangsan@example.com",
        Status:    "active",
        CreatedAt: time.Now(),
        UpdatedAt: time.Now(),
    }
    
    if err := db.Create(user); err != nil {
        panic(err)
    }
    
    fmt.Printf("创建用户成功: %+v\n", user)
    
    // 查询数据
    var foundUser User
    if err := db.First(&foundUser, bson.M{"_id": user.ID}); err != nil {
        panic(err)
    }
    
    fmt.Printf("查询用户成功: %+v\n", foundUser)
    
    // 更新数据
    if err := db.Model(&User{}).Where(bson.M{"_id": user.ID}).Update(bson.M{"name": "李四"}); err != nil {
        panic(err)
    }
    
    // 删除数据
    if err := db.Delete(&User{Id: user.ID}); err != nil {
        panic(err)
    }
    
    fmt.Println("操作完成")
}
```

## API 参考

### DB 结构体方法

| 方法名 | 描述 |
|-------|------|
| `New()` | 创建新的数据库实例 |
| `Start(dbname string, address interface{}) error` | 启动数据库连接 |
| `Close() error` | 关闭数据库连接 |
| `Session(session *Session) *DB` | 创建新的数据库会话 |
| `Database(dbname string) *DB` | 切换数据库 |
| `Collection(model any) (tx *DB, coll *mongo.Collection)` | 获取集合 |
| `BulkWrite(model any, filter ...BulkWriteUpdateFilter) *BulkWrite` | 创建批量写操作 |
| `WithContext(ctx context.Context) *DB` | 设置上下文 |
| `Create(value interface{}) error` | 创建文档 |
| `First(dest interface{}, conds ...interface{}) error` | 查询单个文档 |
| `Find(dest interface{}, conds ...interface{}) error` | 查询多个文档 |
| `Update(update interface{}, conds ...interface{}) error` | 更新文档 |
| `Delete(conds ...interface{}) error` | 删除文档 |
| `AutoMigrator(dst ...interface{}) error` | 自动迁移 |
| `Begin() *DB` | 开始事务 |
| `Commit() error` | 提交事务 |
| `Rollback() error` | 回滚事务 |

### PoolManager 方法

| 方法名 | 描述 |
|-------|------|
| `NewPoolManager(uri string) *PoolManager` | 创建连接池管理器 |
| `Start()` | 启动连接池 |
| `CheckNow() *HealthStatus` | 立即执行健康检查 |
| `IsHealthy() bool` | 检查连接是否健康 |
| `WaitForHealthy(ctx context.Context, timeout time.Duration) bool` | 等待连接恢复健康 |
| `PrepareForBulkOperation(ctx context.Context) error` | 为批量操作做准备 |
| `Execute(ctx context.Context, operation func(*mongo.Client) error) error` | 安全执行数据库操作 |

### clause.Query 方法

| 方法名 | 描述 |
|-------|------|
| `New() *Query` | 创建查询条件 |
| `Eq(k string, v interface{})` | 等于条件 |
| `Gt(k string, v interface{})` | 大于条件 |
| `Gte(k string, v interface{})` | 大于等于条件 |
| `Lt(k string, v interface{})` | 小于条件 |
| `Lte(k string, v interface{})` | 小于等于条件 |
| `Ne(k string, v interface{})` | 不等于条件 |
| `In(k string, v interface{})` | 包含条件 |
| `Nin(k string, v interface{})` | 不包含条件 |
| `OR(v ...*Node)` | OR 条件 |
| `AND(v ...*Node)` | AND 条件 |
| `NOT(v ...*Node)` | NOT 条件 |
| `NOR(v ...*Node)` | NOR 条件 |

### update.Update 方法

| 方法名 | 描述 |
|-------|------|
| `New() Update` | 创建更新操作 |
| `NewFromMap(v map[string]any) Update` | 从映射创建更新操作 |
| `Set(k string, v interface{})` | 设置字段 |
| `Inc(k string, v interface{})` | 递增字段 |
| `Unset(k string)` | 删除字段 |
| `SetOnInert(k string, v interface{})` | 插入时设置字段 |
| `Min(k string, v interface{})` | 最小值字段 |
| `Max(k string, v interface{})` | 最大值字段 |
| `Pop(k string, v interface{})` | 弹出数组元素 |
| `Pull(k string, v interface{})` | 移除数组元素 |
| `Push(k string, v interface{})` | 添加数组元素 |
| `MSet(vs map[string]any)` | 批量设置字段 |

## 最佳实践

1. **使用连接池管理**：始终使用 `PoolManager` 来管理数据库连接，而不是直接创建 `mongo.Client`。

2. **预注册模型**：在应用启动时预注册所有模型，以便自动创建索引。

3. **使用事务**：对于需要原子性的操作，使用事务来确保数据一致性。

4. **合理使用缓存**：对于频繁访问的数据，使用缓存来提高性能。

5. **使用查询条件构建器**：使用 `clause` 包来构建复杂的查询条件，而不是直接使用 `bson.M`。

6. **使用更新操作构建器**：使用 `update` 包来构建更新操作，而不是直接使用 `bson.M`。

7. **定期检查连接健康状态**：使用 `PoolManager` 的健康检查功能来确保连接池的可用性。

8. **处理错误**：始终检查并处理数据库操作的错误。

## 贡献

欢迎提交 Issue 和 Pull Request。

## 许可证

MIT License
