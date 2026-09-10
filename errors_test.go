package cosmo

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hwcer/cosgo/values"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// 模拟真实线上的E11000错误, 消息中包含集合名、索引名、重复键值
const rawDuplicateKeyMessage = `E11000 duplicate key error collection: elf-local-alexjin#S1.guild index: _idx_guild_name_alive dup key: { name: "test" }`

var sensitiveKeywords = []string{"E11000", "collection:", "guild", "_idx_guild_name_alive", "dup key", "test"}

func dupWriteException(messages ...string) error {
	var wes mongo.WriteErrors
	for i, m := range messages {
		wes = append(wes, mongo.WriteError{Index: i, Code: int(CodeDuplicateKey), Message: m})
	}
	return mongo.WriteException{WriteErrors: wes}
}

func assertNoSensitiveInfo(t *testing.T, err error) {
	t.Helper()
	msg := err.Error()
	for _, kw := range sensitiveKeywords {
		if strings.Contains(msg, kw) {
			t.Fatalf("error message leaks database detail %q: %s", kw, msg)
		}
	}
}

func assertMessage(t *testing.T, err error, code int32, text string, args ...string) *values.Message {
	t.Helper()
	m, ok := err.(*values.Message)
	if !ok {
		t.Fatalf("expected *values.Message, got %T: %v", err, err)
	}
	if m.Code != code {
		t.Fatalf("expected code %d, got %d", code, m.Code)
	}
	if m.Error() != text {
		t.Fatalf("expected text %q, got %q", text, m.Error())
	}
	got := DuplicateKeyFields(err)
	if len(args) == 0 {
		args = nil
	}
	if len(got) == 0 {
		got = nil
	}
	if !reflect.DeepEqual(got, args) {
		t.Fatalf("expected fields %v, got %v (Args=%v)", args, got, m.Args)
	}
	return m
}

func TestNormalizeErrorDuplicateKeyWriteException(t *testing.T) {
	err := NormalizeError(dupWriteException(rawDuplicateKeyMessage))

	assertNoSensitiveInfo(t, err)
	assertMessage(t, err, CodeDuplicateKey, ErrDuplicateKey.Error(), "name")

	if !IsDuplicateKey(err) {
		t.Fatal("IsDuplicateKey should be true")
	}
	if !IsDuplicateKey(err, "name") {
		t.Fatal("IsDuplicateKey(err, name) should be true")
	}
	if IsDuplicateKey(err, "_id") {
		t.Fatal("IsDuplicateKey(err, _id) should be false, the conflict is on name")
	}
	if !IsBusinessError(err) {
		t.Fatal("IsBusinessError should be true for duplicate key error")
	}
}

func TestNormalizeErrorDuplicateKeyOnID(t *testing.T) {
	err := NormalizeError(dupWriteException(`E11000 duplicate key error collection: game.role index: _id_ dup key: { _id: 1001 }`))

	assertMessage(t, err, CodeDuplicateKey, ErrDuplicateKey.Error(), "_id")
	if !IsDuplicateKey(err, "_id") {
		t.Fatal("IsDuplicateKey(err, _id) should be true")
	}
	if strings.Contains(err.Error(), "1001") {
		t.Fatal("duplicate key value must not be exposed")
	}
}

func TestNormalizeErrorDuplicateKeyCompoundIndex(t *testing.T) {
	err := NormalizeError(dupWriteException(`E11000 duplicate key error collection: game.guild index: guid_1_name_1 dup key: { guid: 7, name: "x" }`))

	assertMessage(t, err, CodeDuplicateKey, ErrDuplicateKey.Error(), "guid", "name")
	if !IsDuplicateKey(err, "name") {
		t.Fatal("name is part of the conflicting key")
	}
	if !IsDuplicateKey(err, "guid", "name") {
		t.Fatal("both fields are part of the conflicting key")
	}
	if IsDuplicateKey(err, "name", "level") {
		t.Fatal("level is not part of the conflicting key")
	}
}

func TestNormalizeErrorDuplicateKeyTrickyValues(t *testing.T) {
	msg := `E11000 duplicate key error collection: game.guild index: idx dup key: { name: "a, b: {c} \" d", meta: { x: 1, y: [1, 2] }, oid: ObjectId('5f1a2b3c'), tags: ["p: q", "r"] }`
	err := NormalizeError(dupWriteException(msg))

	assertMessage(t, err, CodeDuplicateKey, ErrDuplicateKey.Error(), "name", "meta", "oid", "tags")
}

func TestNormalizeErrorDuplicateKeyLegacyFormat(t *testing.T) {
	// 老版本 MongoDB 的错误信息不含字段名, 回退为索引名
	err := NormalizeError(dupWriteException(`E11000 duplicate key error index: game.guild.$name_1 dup key: { : "x" }`))

	assertMessage(t, err, CodeDuplicateKey, ErrDuplicateKey.Error(), "name_1")
}

func TestNormalizeErrorDuplicateKeyBulkWriteException(t *testing.T) {
	err := NormalizeError(mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{
		{WriteError: mongo.WriteError{Index: 0, Code: int(CodeDuplicateKey), Message: `E11000 duplicate key error collection: g.guild index: name_1 dup key: { name: "a" }`}},
		{WriteError: mongo.WriteError{Index: 1, Code: int(CodeDuplicateKey), Message: `E11000 duplicate key error collection: g.guild index: name_1 dup key: { name: "b" }`}},
		{WriteError: mongo.WriteError{Index: 2, Code: int(CodeDuplicateKey), Message: `E11000 duplicate key error collection: g.guild index: guid_1 dup key: { guid: 3 }`}},
	}})

	assertMessage(t, err, CodeDuplicateKey, ErrDuplicateKey.Error(), "name", "guid")
}

func TestNormalizeErrorDuplicateKeyClientBulkWriteException(t *testing.T) {
	// client.BulkWrite(BulkWrite8) 返回的异常没有实现 mongo.ServerError, 必须由 NormalizeError 自行识别
	err := NormalizeError(mongo.ClientBulkWriteException{WriteErrors: map[int]mongo.WriteError{
		1: {Index: 1, Code: int(CodeDuplicateKey), Message: `E11000 duplicate key error collection: g.guild index: name_1 dup key: { name: "a" }`},
	}})

	assertMessage(t, err, CodeDuplicateKey, ErrDuplicateKey.Error(), "name")
	if !IsBusinessError(err) {
		t.Fatal("IsBusinessError should be true")
	}
}

func TestNormalizeErrorDuplicateKeyCodes(t *testing.T) {
	update := NormalizeError(mongo.CommandError{Code: CodeDuplicateKeyUpdate, Message: `E11001 duplicate key on update index: g.guild.$name_1 dup key: { : "a" }`})
	assertMessage(t, update, CodeDuplicateKeyUpdate, ErrDuplicateKey.Error(), "name_1")

	capped := NormalizeError(mongo.CommandError{Code: CodeDuplicateKeyCapped, Message: `E11000 duplicate key error collection: g.log index: _id_ dup key: { _id: 1 }`})
	assertMessage(t, capped, CodeDuplicateKeyCapped, ErrDuplicateKey.Error(), "_id")

	mongos := NormalizeError(mongo.CommandError{Code: CodeDuplicateKeyMongos, Message: `insert failed :: caused by :: E11000 duplicate key error collection: g.guild index: name_1 dup key: { name: "a" }`})
	assertMessage(t, mongos, CodeDuplicateKeyMongos, ErrDuplicateKey.Error(), "name")

	// 16460 不带 E11000 时不是唯一键冲突, 按普通服务端错误处理: 保留原始错误码和原始信息
	other := NormalizeError(mongo.CommandError{Code: CodeDuplicateKeyMongos, Message: "some other mongos error"})
	if other.Code != CodeDuplicateKeyMongos || !strings.Contains(other.Error(), "some other mongos error") {
		t.Fatalf("unexpected conversion: code=%d text=%q", other.Code, other.Error())
	}
	if IsDuplicateKey(other) || IsBusinessError(other) || DuplicateKeyFields(other) != nil {
		t.Fatal("16460 without E11000 must not be treated as duplicate key")
	}
}

func TestNormalizeErrorDataType(t *testing.T) {
	local := NormalizeError(errors.New("bad value type string for field guild.level, expected int32"))
	m := assertMessage(t, local, CodeTypeMismatch, ErrInvalidDataType.Error())
	if strings.Contains(m.Error(), "guild.level") {
		t.Fatalf("error message leaks field name: %s", m.Error())
	}
	if !IsBusinessError(local) {
		t.Fatal("IsBusinessError should be true for data type error")
	}
	if IsDuplicateKey(local) {
		t.Fatal("data type error is not a duplicate key error")
	}

	// 服务端返回的类型错误保留原始错误码
	server := NormalizeError(mongo.CommandError{Code: CodeBadValue, Message: "BSON field 'level' type mismatch, expected int"})
	assertMessage(t, server, CodeBadValue, ErrInvalidDataType.Error())
}

func TestNormalizeErrorOtherErrors(t *testing.T) {
	if NormalizeError(nil) != nil {
		t.Fatal("nil should stay nil")
	}

	// 驱动本地网络错误: 统一使用 CodeHostUnreachable, 文案保留原始信息便于排查
	network := NormalizeError(errors.New("dial tcp 127.0.0.1:27017: connection refused"))
	if network.Code != CodeHostUnreachable || network.Error() != "dial tcp 127.0.0.1:27017: connection refused" {
		t.Fatalf("unexpected conversion: code=%d text=%q", network.Code, network.Error())
	}
	if !IsNetworkError(network) || IsBusinessError(network) {
		t.Fatal("network error should be classified as network error only")
	}

	// 驱动标签标记的网络错误(没有服务端错误码)
	labeled := NormalizeError(mongo.CommandError{Labels: []string{"NetworkError"}, Message: "connection() error occurred during connection handshake"})
	if labeled.Code != CodeHostUnreachable || !IsNetworkError(labeled) {
		t.Fatalf("labeled network error should map to CodeHostUnreachable, got %d", labeled.Code)
	}

	// 超时
	timeout := NormalizeError(context.DeadlineExceeded)
	if timeout.Code != CodeNetworkTimeout || !IsNetworkError(timeout) {
		t.Fatalf("timeout should map to CodeNetworkTimeout, got %d", timeout.Code)
	}

	// 服务端超时保留原始错误码 50, 仍然视为可重试的网络类错误
	maxTime := NormalizeError(mongo.CommandError{Code: CodeMaxTimeMSExpired, Name: "MaxTimeMSExpired", Message: "operation exceeded time limit"})
	if maxTime.Code != CodeMaxTimeMSExpired || !IsNetworkError(maxTime) {
		t.Fatalf("MaxTimeMSExpired should keep code 50 and be a network error, got %d", maxTime.Code)
	}

	// 其它服务端错误保留原始错误码和原始信息
	conflict := NormalizeError(mongo.CommandError{Code: CodeIndexOptionsConflict, Message: "Index already exists with a different name"})
	if conflict.Code != CodeIndexOptionsConflict || !strings.Contains(conflict.Error(), "Index already exists with a different name") {
		t.Fatalf("unexpected conversion: code=%d text=%q", conflict.Code, conflict.Error())
	}

	// 未知错误使用默认错误码, 文案保留
	plain := NormalizeError(errors.New("something else"))
	if plain.Code != values.MessageErrorCodeDefault || plain.Error() != "something else" {
		t.Fatalf("unexpected conversion: code=%d text=%q", plain.Code, plain.Error())
	}
	if IsDuplicateKey(plain) || IsBusinessError(plain) || IsNetworkError(plain) || DuplicateKeyFields(plain) != nil {
		t.Fatal("unrelated error must not be classified")
	}
}

func TestErrorNilSafety(t *testing.T) {
	db := New()
	if db.Error != nil || db.Err() != nil {
		t.Fatal("new DB should have no error")
	}

	// 空指针 *values.Message 传入判断助手不能 panic, 且一律视为无错误
	var none *values.Message
	if IsBusinessError(none) || IsDuplicateKey(none) || IsDuplicateKey(none, "_id") || IsNetworkError(none) || DuplicateKeyFields(none) != nil {
		t.Fatal("nil *values.Message must be treated as no error")
	}
	if NormalizeError(none) != nil {
		t.Fatal("NormalizeError(nil *values.Message) should return nil")
	}

	// 直接把 nil 的 *values.Message 赋给 error 接口会得到非 nil 接口, Err() 用来规避这个陷阱
	var trap error = db.Error
	if trap == nil {
		t.Fatal("expected the classic nil pointer in interface trap, Err() exists to avoid it")
	}

	db.Errorf("boom")
	if db.Err() == nil || db.Err().Error() != "boom" {
		t.Fatalf("Err() should return the error, got %v", db.Err())
	}
}

func TestSentinelErrors(t *testing.T) {
	db := New().Errorf(ErrInvalidValue)
	if db.Error != ErrInvalidValue {
		t.Fatal("cosmo sentinel should be stored as the same pointer")
	}
	if !errors.Is(db.Err(), ErrInvalidValue) {
		t.Fatal("errors.Is should work for cosmo sentinels")
	}
	for _, sentinel := range []*values.Message{ErrMissingWhereClause, ErrInvalidValue, ErrSelectOnOmitsExist, ErrOmitOnSelectsExist} {
		if sentinel.Code != values.MessageErrorCodeDefault {
			t.Fatalf("sentinel %q should use the default code, got %d", sentinel.Error(), sentinel.Code)
		}
	}
}

func TestSessionKeepsError(t *testing.T) {
	db := New().Errorf("boom")
	tx := db.Session(&Session{})
	if tx.Error != db.Error {
		t.Fatal("Session should carry over the error")
	}
}

func TestNormalizeErrorIdempotentAndSentinelUntouched(t *testing.T) {
	once := NormalizeError(dupWriteException(rawDuplicateKeyMessage))
	if twice := NormalizeError(once); twice != once {
		t.Fatal("already normalized error should be returned as is")
	}

	business := values.Errorf(7001, "guild name taken")
	if got := NormalizeError(business); got != business {
		t.Fatal("business values.Message should be returned as is")
	}

	// 转换过程必须是写时复制, 不能污染包级哨兵
	if ErrDuplicateKey.Code != CodeDuplicateKey || len(ErrDuplicateKey.Args) != 0 {
		t.Fatalf("ErrDuplicateKey sentinel was mutated: code=%d args=%v", ErrDuplicateKey.Code, ErrDuplicateKey.Args)
	}
	if ErrInvalidDataType.Code != CodeTypeMismatch || len(ErrInvalidDataType.Args) != 0 {
		t.Fatalf("ErrInvalidDataType sentinel was mutated: code=%d args=%v", ErrInvalidDataType.Code, ErrInvalidDataType.Args)
	}
}

func TestDuplicateKeyFieldsSurviveJSON(t *testing.T) {
	// 错误经 RPC 序列化后业务层仍能判断冲突字段
	b, err := json.Marshal(NormalizeError(dupWriteException(rawDuplicateKeyMessage)))
	if err != nil {
		t.Fatal(err)
	}
	var m values.Message
	if err = json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	assertMessage(t, &m, CodeDuplicateKey, ErrDuplicateKey.Error(), "name")
	if !IsDuplicateKey(&m, "name") {
		t.Fatal("IsDuplicateKey should work after JSON round trip")
	}
}

func TestRawDriverErrorHelpers(t *testing.T) {
	// 业务层拿到未经转换的驱动原始错误时, 判断助手同样可用
	raw := dupWriteException(`E11000 duplicate key error collection: g.guild index: _id_ dup key: { _id: 1 }`)
	if !IsDuplicateKey(raw, "_id") {
		t.Fatal("IsDuplicateKey should work on raw driver error")
	}
	if got := DuplicateKeyFields(raw); !reflect.DeepEqual(got, []string{"_id"}) {
		t.Fatalf("unexpected fields %v", got)
	}
}

func TestErrorfNormalizesMongoError(t *testing.T) {
	db := New().Errorf(dupWriteException(rawDuplicateKeyMessage))

	if db.Error == nil {
		t.Fatal("db.Error should be set")
	}
	assertNoSensitiveInfo(t, db.Error)
	assertMessage(t, db.Error, CodeDuplicateKey, ErrDuplicateKey.Error(), "name")
	if !IsBusinessError(db.Error) {
		t.Fatal("IsBusinessError(db.Error) should be true")
	}
}

func TestParseDuplicateKeyFieldsMalformedMessages(t *testing.T) {
	// 截断的消息: 值部分不完整, 已解析出的键仍有效, 不能panic
	fields := parseDuplicateKeyFields(`E11000 duplicate key error collection: g.guild index: idx_name dup key: { name: "a", `)
	if !reflect.DeepEqual(fields, []string{"name"}) {
		t.Fatalf("unexpected fields from truncated message: %v", fields)
	}

	// 括号不匹配: 已解析出的键仍有效
	fields = parseDuplicateKeyFields(`E11000 duplicate key error collection: g.guild index: idx_name dup key: { name: "a" `)
	if !reflect.DeepEqual(fields, []string{"name"}) {
		t.Fatalf("unexpected fields from unbalanced message: %v", fields)
	}

	// 完全没有 dup key 标记
	if fields = parseDuplicateKeyFields("E11000 something else"); fields != nil {
		t.Fatalf("unexpected fields: %v", fields)
	}

	// 没有字段也没有索引名: 冲突成立但字段未知
	err := NormalizeError(dupWriteException(`E11000 duplicate key error dup key: { : 1 }`))
	if !IsDuplicateKey(err) || len(DuplicateKeyFields(err)) != 0 {
		t.Fatalf("unknown-field conflict should still be a duplicate key, fields=%v", DuplicateKeyFields(err))
	}
}

func TestNormalizeErrorMixedBulkWrite(t *testing.T) {
	// 无序批量写入: 唯一键冲突与其他错误混合, 冲突成立, Code 取第一个冲突码
	err := NormalizeError(mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{
		{WriteError: mongo.WriteError{Index: 0, Code: 121, Message: "Document failed validation"}},
		{WriteError: mongo.WriteError{Index: 1, Code: int(CodeDuplicateKey), Message: `E11000 duplicate key error collection: g.guild index: name_1 dup key: { name: "a" }`}},
	}})
	if !IsDuplicateKey(err, "name") {
		t.Fatal("mixed batch containing a duplicate key should be recognized")
	}
	assertNoSensitiveInfo(t, err)
}

func TestCacheReloadNormalizes(t *testing.T) {
	c := NewCache(stubCacheHandle{err: errors.New("dial tcp 10.0.0.1:27017: connection refused")})
	err := c.Reload(time.Now().Unix() + 10)
	m, ok := err.(*values.Message)
	if !ok {
		t.Fatalf("expected *values.Message, got %T", err)
	}
	if m.Code != CodeHostUnreachable || !IsNetworkError(m) {
		t.Fatalf("unexpected code %d", m.Code)
	}

	// 已是 Message 的错误原样返回
	business := values.Errorf(7001, "guild name taken")
	c2 := NewCache(stubCacheHandle{err: business})
	if got := c2.Reload(time.Now().Unix() + 10); got != business {
		t.Fatalf("business message should pass through, got %v", got)
	}
}

type stubCacheHandle struct {
	err error
}

func (s stubCacheHandle) Reload(ts int64, cb CacheSetter) error {
	return s.err
}

func TestErrorfStringFormat(t *testing.T) {
	db := New().Errorf("table %s not found", "guild")
	if db.Error == nil || db.Error.Error() != "table guild not found" {
		t.Fatalf("unexpected error: %v", db.Error)
	}
	if db.Error.Code != values.MessageErrorCodeDefault || len(db.Error.Args) != 0 {
		t.Fatalf("formatted internal error should use default code without Args, got code=%d args=%v", db.Error.Code, db.Error.Args)
	}
}
