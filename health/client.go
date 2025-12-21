package health

import (
	"context"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
	MongoTagName     = "BuildUpdate"
	MongoPrimaryKey  = "_id"
	MongoSetOnInsert = "$setOnInsert"
)

/*
NewClient

uri实例  mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[dbname][?options]]

mongodb:// 前缀，代表这是一个Connection String

username:password@ 如果启用了鉴权，需要指定用户密码

hostX:portX 多个 mongos 的地址列表

/dbname 鉴权时，用户帐号所属的数据库

?options 指定额外的连接选项

read preference

	1）primary ： 主节点，默认模式，读操作只在主节点，如果主节点不可用，报错或者抛出异常。

	2）primaryPreferred：首选主节点，大多情况下读操作在主节点，如果主节点不可用，如故障转移，读操作在从节点。

	3）secondary：从节点，读操作只在从节点， 如果从节点不可用，报错或者抛出异常。

	4）secondaryPreferred：首选从节点，大多情况下读操作在从节点，特殊情况（如单主节点架构）读操作在主节点。

	5）nearest：最邻近节点，读操作在最邻近的成员，可能是主节点或者从节点。
*/
func NewClient(address string, opts ...*options.ClientOptions) (client *mongo.Client, err error) {
	if !strings.HasPrefix(address, "mongodb") {
		address = "mongodb://" + address
	}
	c := options.Client().ApplyURI(address)

	// 连接池配置
	c.SetMinPoolSize(20)                  // 最小连接池大小，确保基础并发能力
	c.SetMaxPoolSize(200)                 // 最大连接池大小，根据服务器资源和并发需求调整
	c.SetMaxConnIdleTime(5 * time.Minute) // 连接最大空闲时间，避免资源浪费

	// 超时配置
	c.SetConnectTimeout(10 * time.Second)         // 连接超时时间
	c.SetSocketTimeout(30 * time.Second)          // 套接字超时时间，处理复杂查询
	c.SetServerSelectionTimeout(15 * time.Second) // 服务器选择超时时间
	c.SetHeartbeatInterval(5 * time.Second)       // 心跳检测间隔，快速发现节点变化

	// 重试机制
	c.SetRetryWrites(true) // 启用写操作重试
	c.SetRetryReads(true)  // 启用读操作重试
	// 注意：SetRetryAttempts和SetRetryInterval方法在当前驱动版本中不可用
	// 如需配置重试策略，请使用对应的重试选项结构体

	// 读取偏好 - 单节点数据库应使用primary
	// 对于副本集环境，可根据业务需求选择其他模式
	c.SetReadPreference(readpref.Primary())

	// 连接模式 - 单节点数据库应使用direct模式
	// 对于副本集或分片集群，可设置为false启用负载均衡
	c.SetDirect(true)

	client, err = mongo.Connect(context.Background(), append([]*options.ClientOptions{c}, opts...)...)
	if err != nil {
		return
	}
	if err = client.Ping(context.Background(), nil); err != nil {
		return
	}
	return
}

func NewClientOptions() *options.ClientOptions {
	opts := &options.ClientOptions{}
	return opts
}
