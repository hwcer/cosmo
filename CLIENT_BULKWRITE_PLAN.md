# Cosmo MongoDB 8.0 升级 — BulkWrite8 跨集合批量写入

## Context

MongoDB 8.0 引入 `client.BulkWrite()`，支持跨集合的批量写入（一次网络往返，原子提交）。当前 cosmo 仅支持集合级 `collection.BulkWrite()`，每个集合独立提交。升级后可实现：玩家的 role、items、tasks 等不同集合的变更合并为一次 BulkWrite 提交。

mongo-driver v2.5.0 已提供完整的 Client BulkWrite API，无需升级驱动版本。

## 已完成

### `bulkWrite8.go` — BulkWrite8 实现

```go
type BulkWrite8 struct {
    tx      *DB                        // 克隆的 DB 实例（getInstance）
    ctx     context.Context            // 默认 context.Background()
    writes  []mongo.ClientBulkWrite    // 待提交操作列表
    opts    []options.Lister[...]      // 选项（默认 unordered）
    result  *mongo.ClientBulkWriteResult
    schemas map[reflect.Type]*bulkWrite8Schema  // schema 缓存
    Error   error
}
```

**方法：**

| 方法 | 签名 | 说明 |
|------|------|------|
| `Update` | `(model, data any, where ...any)` | 更新（跳过零值） |
| `Save` | `(model, data any, where ...any)` | 保存（含零值） |
| `Unset` | `(model any, keys []string, where ...any)` | 移除字段 |
| `Insert` | `(model any, documents ...any)` | 插入文档 |
| `Delete` | `(model any, where ...any)` | 删除文档 |
| `Submit` | `() error` | 原子提交所有操作 |
| `Size` | `() int` | 待提交数量 |
| `Result` | `() *ClientBulkWriteResult` | 提交结果 |
| `Options` | `(...Lister[ClientBulkWriteOptions])` | 设置选项 |
| `String` | `() string` | JSON 描述 |

**内部方法：**
- `resolve(model)` — 解析 model 获取 table + schema，按 reflect.Type 缓存

### `cosmo.go` — 工厂方法

```go
func (db *DB) BulkWrite8() *BulkWrite8 {
    return &BulkWrite8{
        tx:      db.getInstance(),  // 克隆，避免污染原始 DB
        ctx:     context.Background(),
        schemas: make(map[reflect.Type]*bulkWrite8Schema),
    }
}
```

### 使用示例

```go
bw8 := db.BulkWrite8()

bw8.Update(&Role{}, data, uid)
bw8.Insert(&Items{}, newItem)
bw8.Unset(&Items{}, []string{"attach"}, oid)
bw8.Delete(&Items{}, oid)

err := bw8.Submit()  // 一次网络往返，原子提交
```

### 设计要点

- **向后兼容**：现有集合级 `BulkWrite` 不变
- **DB 克隆**：工厂方法用 `getInstance()` 克隆，避免污染原始 DB
- **dbname 统一**：通过 `bw8.tx.dbname` 获取，无冗余字段
- **schema 缓存**：按 `reflect.Type` 缓存，避免重复解析
- **filter 复用**：复用 clause/update 包构建 filter 和 update
- **ModelBulkWriteFilter**：自动检测模型是否实现过滤接口

## 后续工作

- **单元测试**：创建跨集合的 BulkWrite8 测试用例，验证原子提交
