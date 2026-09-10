package cosmo

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/hwcer/cosgo/values"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// cosmo 自身的使用错误, 属于编码错误而非业务状态, 使用默认错误码
var (
	ErrMissingWhereClause = values.Errorf(0, "WHERE conditions required")

	ErrInvalidValue = values.Errorf(0, "invalid value, should be pointer to struct or slice")

	ErrSelectOnOmitsExist = values.Errorf(0, "select on omits exist")

	ErrOmitOnSelectsExist = values.Errorf(0, "omit on selects exist")
)

// MongoDB服务端错误码, 与 values.Message.Code 直接对应, 业务层可按码判断
const (
	CodeBadValue     int32 = 2  // BadValue 参数值非法
	CodeTypeMismatch int32 = 14 // TypeMismatch 字段类型不匹配
	CodeInvalidBSON  int32 = 22 // InvalidBSON

	CodeHostUnreachable  int32 = 6  // HostUnreachable 网络不可达, 驱动本地的网络错误统一使用此码
	CodeMaxTimeMSExpired int32 = 50 // MaxTimeMSExpired 服务端操作超时
	CodeNetworkTimeout   int32 = 89 // NetworkTimeout 网络超时, 驱动本地的超时错误统一使用此码

	CodeIndexOptionsConflict int32 = 85 // IndexOptionsConflict 同名索引已存在但选项不同

	CodeDuplicateKey       int32 = 11000 // 插入导致唯一索引重复(E11000)
	CodeDuplicateKeyUpdate int32 = 11001 // 更新导致唯一索引重复(E11001)
	CodeDuplicateKeyCapped int32 = 12582 // capped集合插入导致唯一索引重复
	CodeDuplicateKeyMongos int32 = 16460 // mongos分片转发插入导致唯一索引重复, 错误信息中带E11000
)

// 统一对外暴露的数据库错误
//
//	MongoDB原始错误信息中包含集合名、索引名、重复键值等数据库内部细节, 不允许直接透传给上层调用方
//	NormalizeError 会把驱动层错误转换为带原始错误码的 values.Message, 文案固定, 参数只进 Args
var (
	// ErrDuplicateKey 唯一索引冲突
	//
	//	Code 为实际的错误码(CodeDuplicateKey / CodeDuplicateKeyUpdate / CodeDuplicateKeyCapped / CodeDuplicateKeyMongos)
	//	Args 为冲突的字段名列表(按索引键顺序), 如 ["_id"] 或 ["guid","name"], 业务层据此判断是ID重复还是哪个字段重复
	ErrDuplicateKey = values.Errorf(CodeDuplicateKey, "duplicate key error")

	// ErrInvalidDataType 数据类型错误
	//
	//	服务端返回 CodeBadValue / CodeTypeMismatch / CodeInvalidBSON 时使用原始错误码, 驱动本地的编解码错误统一使用 CodeTypeMismatch
	ErrInvalidDataType = values.Errorf(CodeTypeMismatch, "invalid data type")
)

// 数据类型错误关键词, 此类错误信息中会暴露字段名等数据库内部细节
var dataTypeErrors = []string{
	"bad value type",
	"BSON type",
	"cannot convert",
	"type mismatch",
	"invalid type",
}

// 网络错误关键词, 补充驱动标签判断, 确保全面覆盖
var networkErrors = []string{
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

// NormalizeError 统一转换驱动层错误, cosmo内所有向上层返回错误的出口都应经过此函数, 结果可直接赋给 DB.Error
//
//	nil                 -> nil
//	*values.Message     -> 原样返回
//	唯一键冲突           -> ErrDuplicateKey(Code为原始错误码, Args为冲突字段名)
//	数据类型错误         -> ErrInvalidDataType
//	其它服务端错误       -> Code为原始错误码, 文案保留原始信息
//	网络错误 / 超时      -> CodeHostUnreachable / CodeNetworkTimeout, 文案保留原始信息
//	其它错误             -> values.MessageErrorCodeDefault, 文案保留原始信息
//
// 注意: 返回值是 *values.Message, 在返回值为 error 的函数中不要直接 return 一个可能为 nil 的结果, 应先判空
func NormalizeError(err error) *values.Message {
	if err == nil {
		return nil
	}
	if m, ok := err.(*values.Message); ok {
		return m
	}

	errs := serverErrors(err)
	if code := duplicateKeyCode(err, errs); code != 0 {
		fields := duplicateKeyFields(err, errs)
		args := make([]any, 0, len(fields))
		for _, f := range fields {
			args = append(args, f)
		}
		return values.Errorf(code, ErrDuplicateKey).WithArgs(args...)
	}

	if isDataTypeError(err) {
		code := CodeTypeMismatch
		for _, e := range errs {
			if isDataTypeCode(e.code) {
				code = e.code
				break
			}
		}
		return values.Errorf(code, ErrInvalidDataType)
	}

	for _, e := range errs {
		if e.code != 0 {
			return values.Errorf(e.code, err)
		}
	}

	if mongo.IsTimeout(err) {
		return values.Errorf(CodeNetworkTimeout, err)
	}
	if mongo.IsNetworkError(err) || hasNetworkKeyword(err.Error()) {
		return values.Errorf(CodeHostUnreachable, err)
	}

	return values.Error(err)
}

// IsBusinessError 检查是不是无法恢复的业务错误
//
//	1、插入或更新时唯一索引重复
//	2、数据类型错误
//
// 同时支持 NormalizeError 转换后的错误和驱动原始错误
func IsBusinessError(err error) bool {
	if err == nil {
		return false
	}
	if m, ok := err.(*values.Message); ok {
		return m != nil && (isDuplicateKeyMessage(m) || isDataTypeCode(m.Code))
	}
	if duplicateKeyCode(err, serverErrors(err)) != 0 {
		return true
	}
	return isDataTypeError(err)
}

// IsDuplicateKey 是否唯一索引冲突
//
//	不传 fields 时只判断是否为唯一键冲突
//	传入 fields 时要求这些字段全部出现在冲突键中, 如 IsDuplicateKey(err, "_id") 判断是否为ID重复
//
// 同时支持 NormalizeError 转换后的错误和驱动原始错误
func IsDuplicateKey(err error, fields ...string) bool {
	if err == nil {
		return false
	}
	var got []string
	switch m := err.(type) {
	case *values.Message:
		if m == nil || !isDuplicateKeyMessage(m) {
			return false
		}
		got = messageArgs(m)
	default:
		errs := serverErrors(err)
		if duplicateKeyCode(err, errs) == 0 {
			return false
		}
		got = duplicateKeyFields(err, errs)
	}
	for _, f := range fields {
		if !containsString(got, f) {
			return false
		}
	}
	return true
}

// DuplicateKeyFields 返回唯一索引冲突涉及的字段名(按索引键顺序), 非唯一键冲突返回nil
//
//	同时支持 NormalizeError 转换后的错误和驱动原始错误
//	老版本MongoDB的错误信息中不含字段名, 此时回退为索引名
func DuplicateKeyFields(err error) []string {
	if err == nil {
		return nil
	}
	if m, ok := err.(*values.Message); ok {
		if m == nil || !isDuplicateKeyMessage(m) {
			return nil
		}
		return messageArgs(m)
	}
	errs := serverErrors(err)
	if duplicateKeyCode(err, errs) == 0 {
		return nil
	}
	return duplicateKeyFields(err, errs)
}

// messageArgs 读取 Message.Args 中冲突字段名, 过一次JSON后数字会变float64, 统一转回字符串
func messageArgs(m *values.Message) []string {
	fields := make([]string, 0, len(m.Args))
	for _, a := range m.Args {
		if s, ok := a.(string); ok {
			fields = append(fields, s)
		} else {
			fields = append(fields, fmt.Sprint(a))
		}
	}
	return fields
}

// IsNetworkError 检查是不是MONGO网络错误或超时, 此类错误通常可以重试
//
//	同时支持 NormalizeError 转换后的错误和驱动原始错误
func IsNetworkError(err error) bool {
	if err == nil {
		return false
	}
	if m, ok := err.(*values.Message); ok {
		if m == nil {
			return false
		}
		switch m.Code {
		case CodeHostUnreachable, CodeNetworkTimeout, CodeMaxTimeMSExpired:
			return true
		}
		return hasNetworkKeyword(m.Error())
	}

	// 优先使用MongoDB官方驱动的网络错误判断
	if mongo.IsNetworkError(err) || mongo.IsTimeout(err) {
		return true
	}
	return hasNetworkKeyword(err.Error())
}

func isDuplicateKeyCode(code int32) bool {
	switch code {
	case CodeDuplicateKey, CodeDuplicateKeyUpdate, CodeDuplicateKeyCapped, CodeDuplicateKeyMongos:
		return true
	}
	return false
}

// isDuplicateKeyMessage 判断转换后的 Message 是否为唯一键冲突
//
//	16460 本身有歧义(只有带E11000时才是唯一键冲突), 转换后的唯一键冲突文案固定为 ErrDuplicateKey, 以此区分
func isDuplicateKeyMessage(m *values.Message) bool {
	switch m.Code {
	case CodeDuplicateKey, CodeDuplicateKeyUpdate, CodeDuplicateKeyCapped:
		return true
	case CodeDuplicateKeyMongos:
		return m.Error() == ErrDuplicateKey.Error()
	}
	return false
}

func isDataTypeCode(code int32) bool {
	switch code {
	case CodeBadValue, CodeTypeMismatch, CodeInvalidBSON:
		return true
	}
	return false
}

func isDataTypeError(err error) bool {
	errorStr := err.Error()
	for _, keyword := range dataTypeErrors {
		if strings.Contains(errorStr, keyword) {
			return true
		}
	}
	return false
}

func hasNetworkKeyword(errorStr string) bool {
	for _, keyword := range networkErrors {
		if strings.Contains(errorStr, keyword) {
			return true
		}
	}
	return false
}

// serverError 服务端返回的单条错误
type serverError struct {
	code    int32
	message string
}

// serverErrors 展开驱动各类异常中的服务端错误
//
//	mongo.ClientBulkWriteException(client.BulkWrite) 没有实现 mongo.ServerError, 驱动自带的 mongo.IsDuplicateKeyError 识别不了, 这里需要单独处理
func serverErrors(err error) (errs []serverError) {
	if ce, ok := errors.AsType[mongo.CommandError](err); ok {
		errs = append(errs, serverError{code: ce.Code, message: ce.Message})
	}

	if we, ok := errors.AsType[mongo.WriteError](err); ok {
		errs = append(errs, serverError{code: int32(we.Code), message: we.Message})
	}

	if wce, ok := errors.AsType[mongo.WriteConcernError](err); ok {
		errs = append(errs, serverError{code: int32(wce.Code), message: wce.Message})
	}

	if wex, ok := errors.AsType[mongo.WriteException](err); ok {
		for _, e := range wex.WriteErrors {
			errs = append(errs, serverError{code: int32(e.Code), message: e.Message})
		}
		if wex.WriteConcernError != nil {
			errs = append(errs, serverError{code: int32(wex.WriteConcernError.Code), message: wex.WriteConcernError.Message})
		}
	}

	if bwe, ok := errors.AsType[mongo.BulkWriteException](err); ok {
		for _, e := range bwe.WriteErrors {
			errs = append(errs, serverError{code: int32(e.Code), message: e.Message})
		}
		if bwe.WriteConcernError != nil {
			errs = append(errs, serverError{code: int32(bwe.WriteConcernError.Code), message: bwe.WriteConcernError.Message})
		}
	}

	if cbwe, ok := errors.AsType[mongo.ClientBulkWriteException](err); ok {
		if cbwe.WriteError != nil {
			errs = append(errs, serverError{code: int32(cbwe.WriteError.Code), message: cbwe.WriteError.Message})
		}
		keys := make([]int, 0, len(cbwe.WriteErrors))
		for k := range cbwe.WriteErrors {
			keys = append(keys, k)
		}
		sort.Ints(keys)
		for _, k := range keys {
			e := cbwe.WriteErrors[k]
			errs = append(errs, serverError{code: int32(e.Code), message: e.Message})
		}
		for _, e := range cbwe.WriteConcernErrors {
			errs = append(errs, serverError{code: int32(e.Code), message: e.Message})
		}
	}
	return
}

// duplicateKeyCode 返回唯一键冲突的实际错误码, 非唯一键冲突返回0
func duplicateKeyCode(err error, errs []serverError) int32 {
	for _, e := range errs {
		switch e.code {
		case CodeDuplicateKey, CodeDuplicateKeyUpdate, CodeDuplicateKeyCapped:
			return e.code
		case CodeDuplicateKeyMongos:
			// 与驱动判断一致: 16460 只有在信息中带E11000时才是唯一键冲突
			if strings.Contains(e.message, "E11000") {
				return e.code
			}
		}
	}
	// 兜底使用驱动的判断, 覆盖未展开的 mongo.ServerError 实现
	if mongo.IsDuplicateKeyError(err) {
		return CodeDuplicateKey
	}
	return 0
}

// duplicateKeyFields 汇总所有唯一键冲突涉及的字段名, 去重并保持顺序
func duplicateKeyFields(err error, errs []serverError) (fields []string) {
	for _, e := range errs {
		if isDuplicateKeyCode(e.code) {
			fields = appendUnique(fields, parseDuplicateKeyFields(e.message)...)
		}
	}
	if len(fields) == 0 {
		fields = appendUnique(fields, parseDuplicateKeyFields(err.Error())...)
	}
	return
}

// parseDuplicateKeyFields 从E11000错误信息中解析冲突的字段名
//
//	新版本: E11000 duplicate key error collection: db.coll index: idx_name dup key: { guid: 1, name: "x" }
//	老版本: E11000 duplicate key error index: db.coll.$idx_name dup key: { : "x" }   不含字段名, 回退为索引名
//
// 一条信息中可能包含多个 dup key(批量写入), 全部解析
func parseDuplicateKeyFields(msg string) (fields []string) {
	const marker = "dup key: {"
	rest := msg
	for {
		i := strings.Index(rest, marker)
		if i < 0 {
			break
		}
		body := rest[i+len(marker):]
		keys, consumed := parseObjectKeys(body)
		if len(keys) == 0 {
			if name := parseIndexName(rest[:i]); name != "" {
				keys = []string{name}
			}
		}
		fields = appendUnique(fields, keys...)
		rest = body[consumed:]
	}
	return
}

// parseObjectKeys 解析 { k1: v1, k2: v2 } 形式文本中第一层的键名, body 为去掉开头 "{" 之后的内容
//
//	值可能是字符串、嵌套文档、数组、ObjectId('...') 等, 通过引号和括号深度跳过
//	返回键名列表和已消费的字节数
func parseObjectKeys(body string) (keys []string, consumed int) {
	depth, start, expectKey := 1, 0, true
	var quote byte
	consumed = len(body)
	for j := 0; j < len(body); j++ {
		c := body[j]
		if quote != 0 {
			if c == '\\' {
				j++
			} else if c == quote {
				quote = 0
			}
			continue
		}
		switch c {
		case '"', '\'':
			quote = c
		case '{', '[':
			depth++
		case '}', ']':
			depth--
			if depth == 0 {
				return keys, j + 1
			}
		case ':':
			if depth == 1 && expectKey {
				if key := strings.Trim(strings.TrimSpace(body[start:j]), `"'`); key != "" {
					keys = append(keys, key)
				}
				expectKey = false
			}
		case ',':
			if depth == 1 {
				expectKey = true
				start = j + 1
			}
		}
	}
	return
}

// parseIndexName 从错误信息中解析索引名
//
//	新版本: index: idx_name
//	老版本: index: db.coll.$idx_name
func parseIndexName(msg string) string {
	const marker = "index: "
	i := strings.LastIndex(msg, marker)
	if i < 0 {
		return ""
	}
	name := msg[i+len(marker):]
	if j := strings.IndexByte(name, ' '); j >= 0 {
		name = name[:j]
	}
	if k := strings.LastIndexByte(name, '$'); k >= 0 {
		name = name[k+1:]
	}
	return name
}

func appendUnique(dst []string, items ...string) []string {
	for _, it := range items {
		if !containsString(dst, it) {
			dst = append(dst, it)
		}
	}
	return dst
}

func containsString(list []string, s string) bool {
	return slices.Contains(list, s)
}
