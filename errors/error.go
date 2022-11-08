package errors

import (
	"fmt"
	"github.com/pkg/errors"
	"io"
	"log"
	"runtime"
)

/**
这边主要的功能就是错误信息加入自定义的消息，并且返回堆栈信息
如果不需要自定义的消息，并且返回最原始的错误来源，直接用 Cause(err)就可以了
*/

func callers() []uintptr {
	var pcs [32]uintptr
	l := runtime.Callers(3, pcs[:])
	return pcs[:l]
}

type Error interface {
	error //error 是一个接口，里面包含一个方法 Error() string
}

type item struct {
	msg   string
	stack []uintptr //堆栈 是一个整数类型，其大小足以容纳任何指针
}

// 实现Error接口里面的Error方法
func (i *item) Error() string {
	return i.msg
}

func (i *item) Format(s fmt.State, ver rune) {
	io.WriteString(s, i.msg) //把i.msg 内容写入到s里面
	io.WriteString(s, "\n")

	for _, pc := range i.stack {
		fmt.Fprintf(s, "%+v\n", errors.Frame(pc))
	}
}

// New create a new error
func New(msg string) Error {
	return &item{msg: msg, stack: callers()}
}

// Errorf create a new error
func Errorf(format string, args ...interface{}) Error {
	return &item{msg: fmt.Sprintf(format, args...), stack: callers()}
}

// Wrap with some extra message into err
func Wrap(err error, msg string) Error {
	if err == nil {
		return nil
	}

	e, ok := err.(*item)
	if !ok {
		return &item{msg: fmt.Sprintf("%s; %s", msg, err.Error()), stack: callers()}
	}

	e.msg = fmt.Sprintf("%s; %s", msg, e.msg)
	return e
}

// Wrapf with some extra message into err
func Wrapf(err error, format string, args ...interface{}) Error {
	if err == nil {
		return nil
	}

	msg := fmt.Sprintf(format, args...)

	e, ok := err.(*item)
	if !ok {
		return &item{msg: fmt.Sprintf("%s; %s", msg, err.Error()), stack: callers()}
	}

	e.msg = fmt.Sprintf("%s; %s", msg, e.msg)
	return e
}

// WithStack add caller stack information
func WithStack(err error) Error {
	if err == nil {
		return nil
	}

	if e, ok := err.(*item); ok {
		return e
	}

	return &item{msg: err.Error(), stack: callers()}
}

func Recover() {
	e := recover()
	if e != nil {
		s := Stack(2)
		log.Fatalf("Panic: %v\nTraceback\r:%s", e, string(s))
	}
}

func RecoverStackWithoutLF() {
	e := recover()
	if e != nil {
		s := StackWithoutLF(3)
		log.Fatalf("Panic: %v Traceback:%s", e, string(s))
	}
}
