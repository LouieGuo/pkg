package errors

import (
	"errors"
	"fmt"
	errors2 "github.com/pkg/errors"
	"testing"
)

func TestError(t *testing.T) {
	//err := errors.New("origin error")
	//err = Wrap(err, "add error") //调用自己定义的，有包含堆栈
	//fmt.Println(err)

	err := testErr1()
	fmt.Println(err)
	//fmt.Println(WithStack(err))
	//fmt.Printf("%+v", err)
	//fmt.Printf("%+v", errors2.Cause(err))
}

func testErr1() error {
	return errors2.Wrap(errors.New("origin error"), "add testErr1")
}
func testErr2() error {
	return testErr1()
}
func testErr3() error {
	return testErr2()
}
