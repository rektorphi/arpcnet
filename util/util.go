package util

import (
	"encoding/base32"
	"encoding/base64"
	"log"
	"math/rand"
	"reflect"
)

var B64 = b64{}

// 6 bits per char, 4 chars for 3 bytes
// chars	bytes
// 1		0/invalid
// 2		1
// 3		2
// 4		3
type b64 struct {
}

// ToInfoStr takes the last 6 bytes from the given number and encodes it into 8 base64 characters.
func (b64) ToInfoStr(id uint64) string {
	var b [6]byte
	b[0] = byte(id)
	b[1] = byte(id >> 8)
	b[2] = byte(id >> 16)
	b[3] = byte(id >> 24)
	b[4] = byte(id >> 32)
	b[5] = byte(id >> 40)
	return base64.URLEncoding.EncodeToString(b[:])
}

func (b64) RandomStr(length int) string {
	bs := make([]byte, (length*3/4)+1)
	rand.Read(bs)
	return base64.URLEncoding.EncodeToString(bs)[:length]
}

var B32 = b32{}

// 5 bits per char
// chars	bytes
// 1		0/invalid
// 2		1
// 3		1/invalid
// 4		2
// 5		3
// 6		3/invalid
// 7		4
// 8		5
type b32 struct {
}

// ToInfoStr takes the last 5 bytes from the given number and encodes it into 8 base32 characters.
func (b32) ToInfoStr(id uint64) string {
	var b [5]byte
	b[0] = byte(id)
	b[1] = byte(id >> 8)
	b[2] = byte(id >> 16)
	b[3] = byte(id >> 24)
	b[4] = byte(id >> 32)
	return base32.StdEncoding.EncodeToString(b[:])
}

func (b32) IToInfoStr(ptr interface{}) string {
	rv := reflect.ValueOf(ptr)
	x := rv.Pointer()
	return B32.ToInfoStr(uint64(x))
}

func (b32) RandomStr(length int) string {
	bs := make([]byte, (length*5/8)+1)
	rand.Read(bs)
	return base32.StdEncoding.EncodeToString(bs)[:length]
}

type Logger interface {
	Printf(f string, args ...interface{})
	WithPrefix(prefix string) Logger
}

type NopLogger struct {
}

func (NopLogger) Printf(f string, args ...interface{}) {
}

func (NopLogger) WithPrefix(prefix string) Logger {
	return NopLogger{}
}

type StdLogger struct {
	*log.Logger
}

func (sl *StdLogger) Printf(f string, args ...interface{}) {
	sl.Logger.Printf(f, args...)
}

func (sl *StdLogger) WithPrefix(prefix string) Logger {
	return &StdLogger{Logger: log.New(sl.Writer(), sl.Prefix()+prefix, sl.Flags())}
}

func NewLoggerFrom(l *log.Logger) *log.Logger {
	return log.New(l.Writer(), l.Prefix(), l.Flags())
}
