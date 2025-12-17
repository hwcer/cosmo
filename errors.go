package cosmo

import (
	"errors"
	"strings"

	"go.mongodb.org/mongo-driver/mongo"
)

var (
	ErrMissingWhereClause = errors.New("WHERE conditions required")

	ErrInvalidValue = errors.New("invalid value, should be pointer to struct or slice")

	ErrSelectOnOmitsExist = errors.New("select on omits exist")

	ErrOmitOnSelectsExist = errors.New("omit on selects exist")
)

// 检查是不是无法恢复的业务错误
//
//	1、插入时主键重复
//
// 2、数据类型错误
func IsBusinessError(err error) bool {
	if err == nil {
		return false
	}

	// 优先使用MongoDB官方驱动的主键重复错误判断
	if mongo.IsDuplicateKeyError(err) {
		return true
	}

	// 检查是否是数据类型错误
	dataTypeErrors := []string{
		"bad value type",
		"BSON type",
		"cannot convert",
		"type mismatch",
		"invalid type",
	}

	errorStr := err.Error()
	for _, keyword := range dataTypeErrors {
		if strings.Contains(errorStr, keyword) {
			return true
		}
	}

	return false
}

// 检查是不是MONGO网络错误
func IsNetworkError(err error) bool {
	if err == nil {
		return false
	}

	// 优先使用MongoDB官方驱动的网络错误判断
	if mongo.IsNetworkError(err) || mongo.IsTimeout(err) {
		return true
	}

	// 补充检查常见的网络错误关键词，确保全面覆盖
	errorKeyWords := []string{
		"connection refused",
		"connection timeout",
		"server selection timeout",
		"socket timeout",
		"network unreachable",
		"no reachable servers",
		"connection reset by peer",
		"i/o timeout",
		"context deadline exceeded",
		"dial tcp",
		"network error",
	}

	// 将错误转换为字符串进行检查
	errorStr := err.Error()
	for _, keyword := range errorKeyWords {
		if strings.Contains(errorStr, keyword) {
			return true
		}
	}

	return false
}
