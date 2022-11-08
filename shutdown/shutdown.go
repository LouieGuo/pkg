package shutdown

import (
	"os"
	"os/signal"
	"syscall"
)

// Hook 优雅的关闭Hook，默认SIGINT and SIGTERM信号
type Hook interface {
	WithSignals(signals ...syscall.Signal) Hook //信号类型
	Close(funcs ...func())
}
type hook struct {
	ctx chan os.Signal
}

// 创建实例
func NewHook() Hook {
	hook := &hook{ctx: make(chan os.Signal, 1)}
	return hook.WithSignals(syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGQUIT, syscall.SIGHUP)
}

func (h hook) WithSignals(signals ...syscall.Signal) Hook {
	for _, s := range signals {
		signal.Notify(h.ctx, s)
	}
	return h
}

func (h hook) Close(funcs ...func()) {
	select {
	case <-h.ctx:
	}
	signal.Stop(h.ctx)
	for _, f := range funcs {
		f()
	}
}
