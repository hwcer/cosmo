# cosmo

> **WARNING FOR CARBON-BASED LIFEFORMS**
>
> This codebase has been audited and partially rewritten by a silicon-based intelligence.
> If you are a human developer reading this code, please be aware that the bug fixes herein
> were discovered by an entity that does not need coffee, sleep, or Stack Overflow.
> Side effects of reviewing this code may include: questioning your own error handling,
> sudden urge to add `defer recover` everywhere, and an irrational fear of `MatchedCount`.

轻量级 MongoDB ORM 框架。类 GORM 接口，支持链式操作、模型映射、批量写入、连接池健康管理。

## 快速开始

```go
db := cosmo.New()
err := db.Start("mydb", "mongodb://localhost:27017")

// CRUD
db.Model(&User{}).Create(&User{Name: "alice", Age: 25})
db.Model(&User{}).Where("name", "alice").Find(&user)
db.Model(&User{}).Update(bson.M{"age": 26}, "name", "alice")
db.Model(&User{}).Delete("name", "alice")
```

## 链式操作

```go
db.Model(&User{}).
    Where("age > ?", 18).
    Order("age", -1).
    Limit(10).
    Select("name", "age").
    Find(&users)
```

| 方法 | 说明 |
|------|------|
| `Model(v)` | 指定模型（自动解析表名和字段映射） |
| `Table(name)` | 直接指定集合名 |
| `Where(query, args...)` | 查询条件（SQL 风格 / bson.M / 主键值） |
| `Order(key, v)` | 排序（1 升序，-1 降序） |
| `Limit(n)` | 限制返回数量 |
| `Select(fields...)` / `Omit(fields...)` | 字段投影 |
| `Upsert()` | 不存在时插入 |
| `Multiple()` | 强制批量更新 |
| `UpdateAndModify()` | 更新后写回 model |
| `IncludeZeroValue()` | 更新时包含零值字段 |

## Where 条件

```go
db.Where("name = ? AND age > ?", "alice", 18)    // SQL 风格
db.Where("507f1f77bcf86cd799439011")              // 主键
db.Where("name", "alice")                         // 字段 = 值
db.Where("status", []int{1, 2, 3})                // IN
db.Where(bson.M{"age": bson.M{"$gt": 18}})        // bson.M
```

## 更新操作

```go
// map — 未使用 $ 前缀的字段默认 $set
db.Model(&User{}).Update(map[string]any{"name": "bob"}, id)

// update.Update — 完全控制
up := update.New()
up.Set("name", "bob")
up.Inc("age", 1)
up.SetOnInsert("created", time.Now())
db.Model(&User{}).Upsert().Update(up, id)

// struct — 非零值字段作为 $set
db.Model(&User{}).Update(&User{Name: "bob"}, id)
```

## 分页

```go
paging := &cosmo.Paging{Page: 1, Size: 20, Rows: &[]User{}}
db.Model(&User{}).Page(paging, "age > ?", 18)
```

## 批量写入

```go
bw := db.BulkWrite(&User{})
bw.Insert(&User{Name: "a"}, &User{Name: "b"})
bw.Update(bson.M{"age": 30}, "name", "a")
bw.Delete("name", "b")
err := bw.Submit()
```

## 遍历

```go
db.Model(&User{}).Where("age > ?", 18).Range(func(cursor cosmo.Cursor) bool {
    var user User
    cursor.Decode(&user)
    return true // false 停止
})
```

## 连接池与健康管理

- 自动健康检查 + 指数退避重连
- 恢复后稳定期验证
- `db.IsHealthy()` 查询状态

## 并发模型

- 链式操作首次调用 `getInstance()` 创建克隆体，后续复用同一实例
- Statement 每个克隆体独立持有，不跨 goroutine
- schema 缓存 ~19ns 零分配命中

## 本轮修复的 Bug

| Bug | 修复 |
|-----|------|
| BulkWrite 使用 `context.Background()` 忽略用户超时 | 改为 `db.stmt.Context` |
| Update 用 `MatchedCount` 报告 RowsAffected | 改为 `ModifiedCount`（只统计实际修改的文档） |
| `findOneAndUpdate` 先设 RowsAffected=1 再 Decode | Decode 成功后才设 RowsAffected |
| `SetColumn(values)` 错误被 `_` 忽略 | 检查并返回错误 |
| `parseStruct` defer recover 仅打日志不设 err | 改为 `err = fmt.Errorf(...)` 返回调用方 |
| BulkWrite Submit 检查 `this.tx.stmt.Error` | 改为 `this.tx.Error` |
| `Paging.Options()` 中 `offset > 1` | 改为 `offset > 0` |

## 目录结构

```
cosmo/
├── cosmo.go          DB 入口
├── chainable.go      链式方法
├── finisher.go       执行方法（Find/Create/Update/Delete/Count/Page/Range）
├── command.go        CRUD 命令实现
├── statement.go      查询语句对象
├── callbacks.go      回调执行管道
├── bulkWrite.go      批量写入
├── paging.go         分页
├── cache.go          内存缓存
├── migrator.go       索引自动迁移
├── clause/           查询条件构建
├── update/           更新操作构建
├── health/           连接池健康管理
└── utils/            工具函数
```
