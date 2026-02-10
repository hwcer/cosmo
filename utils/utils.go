package utils

import (
	"database/sql/driver"

	"fmt"
	"go.mongodb.org/mongo-driver/v2/bson"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"unicode"
)

var gormSourceDir string

func init() {
	_, file, _, _ := runtime.Caller(0)
	// compatible solution to get gorm source directory with various operating systems
	gormSourceDir = regexp.MustCompile(`utils.utils\.go`).ReplaceAllString(file, "")
}

func ToBson(i interface{}) (bson.M, error) {
	switch v := i.(type) {
	case map[string]any:
		return v, nil
	case bson.M:
		return v, nil
	default:
		return nil, fmt.Errorf("cosmo.update ToBson convert error:%v", i)
	}
}

func ValueOf(i interface{}) reflect.Value {
	value, ok := i.(reflect.Value)
	if !ok {
		value = reflect.ValueOf(i)
	}
	return value
}

func ToArray(v interface{}) (r []interface{}) {
	vf := reflect.Indirect(reflect.ValueOf(v))
	if vf.Kind() != reflect.Array && vf.Kind() != reflect.Slice {
		return []interface{}{v}
	}
	for i := 0; i < vf.Len(); i++ {
		r = append(r, vf.Index(i).Interface())
	}
	return
}

// FileWithLineNum return the file name and line number of the current file
func FileWithLineNum() string {
	// the second caller usually from gorm internal, so set i start from 2
	for i := 2; i < 15; i++ {
		_, file, line, ok := runtime.Caller(i)
		if ok && (!strings.HasPrefix(file, gormSourceDir) || strings.HasSuffix(file, "_test.go")) {
			return file + ":" + strconv.FormatInt(int64(line), 10)
		}
	}

	return ""
}

func IsValidDBNameChar(c rune) bool {
	return !unicode.IsLetter(c) && !unicode.IsNumber(c) && c != '.' && c != '*' && c != '_' && c != '$' && c != '@'
}

// CheckTruth check string true or not
func CheckTruth(vals ...string) bool {
	for _, val := range vals {
		if !strings.EqualFold(val, "false") && val != "" {
			return true
		}
	}
	return false
}

func ToStringKey(values ...interface{}) string {
	results := make([]string, len(values))

	for idx, value := range values {
		if valuer, ok := value.(driver.Valuer); ok {
			value, _ = valuer.Value()
		}

		switch v := value.(type) {
		case string:
			results[idx] = v
		case []byte:
			results[idx] = string(v)
		case uint:
			results[idx] = strconv.FormatUint(uint64(v), 10)
		default:
			results[idx] = fmt.Sprint(reflect.Indirect(reflect.ValueOf(v)).Interface())
		}
	}

	return strings.Join(results, "_")
}

func Contains(elems []string, elem string) bool {
	for _, e := range elems {
		if elem == e {
			return true
		}
	}
	return false
}

func AssertEqual(src, dst interface{}) bool {
	if !reflect.DeepEqual(src, dst) {
		if valuer, ok := src.(driver.Valuer); ok {
			src, _ = valuer.Value()
		}

		if valuer, ok := dst.(driver.Valuer); ok {
			dst, _ = valuer.Value()
		}

		return reflect.DeepEqual(src, dst)
	}
	return true
}

func ToString(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case int:
		return strconv.FormatInt(int64(v), 10)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	}
	return ""
}
