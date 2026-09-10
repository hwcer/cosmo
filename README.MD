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

## 错误处理

驱动层错误在返回给上层前统一经 `NormalizeError` 转换为 `*values.Message`(`DB.Error`、`BulkWrite8.Error` 字段即此类型), MongoDB 原始错误中的集合名、索引名、重复键值不会透传给客户端:

| 错误类别 | Code | 文案 |
|---------|------|------|
| 唯一键冲突 | 11000 / 11001 / 12582 / 16460(原始码) | 固定 `duplicate key error`, 冲突字段名在 `Args` |
| 数据类型错误 | 2 / 14 / 22(服务端原始码) | 固定 `invalid data type` |
| 其他服务端错误 | 原始错误码 | 保留原始信息 |
| 网络错误 / 超时 | 6 / 89 / 50 | 保留原始信息 |
| 未知错误 | 9999(默认码) | 保留原始信息 |

判断助手(对转换后的 Message、nil 的 `*values.Message`、驱动原始错误均有效, 经过 RPC JSON 序列化后依然有效):

```go
tx := db.Create(&guild)
if msg := tx.Error; msg != nil {
    switch {
    case cosmo.IsDuplicateKey(msg, "_id"):   // ID 重复
    case cosmo.IsDuplicateKey(msg, "name"):  // 公会名被占用
    case cosmo.IsNetworkError(msg):           // 网络/超时, 可重试
    default:
        logger.Error("code=%d text=%s args=%v", msg.Code, msg.Error(), msg.Args)
    }
}
```

- `cosmo.IsDuplicateKey(err, fields...)`: 是否唯一键冲突; 传入字段名时要求全部出现在冲突键中
- `cosmo.DuplicateKeyFields(err)`: 冲突字段名列表(按索引键顺序; 老版本 MongoDB 不含字段名时回退为索引名)
- `cosmo.IsBusinessError(err)`: 唯一键冲突或数据类型错误, 属于不可恢复的业务错误
- `cosmo.IsNetworkError(err)`: 网络/超时错误, 可重试
- `db.Err()`: 以 `error` 接口返回错误; `DB.Error` 是 `*values.Message` 指针, 直接 return 给 error 会得到非 nil 的空接口

注意: 转换后的 Message 不再保留驱动原始错误的 `errors.Is/As` 链, 对驱动错误类型的判断需改用 Code; 判断唯一键冲突请使用 `IsDuplicateKey`, 不要对转换结果做 `errors.Is(err, cosmo.ErrDuplicateKey)`(`values.Message` 靠 Code 比较而非指针身份)。

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
