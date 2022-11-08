package errors

import (
	"errors"
	"fmt"
	errors2 "github.com/pkg/errors"
	"testing"
)

func TestError(t *testing.T) {
	//err := errors.New("origin error")
	//err = Wrap(err, "add error")
	//fmt.Println(WithStack(err))
	err := testErr3()
	//fmt.Println(WithStack(err))
	//fmt.Printf("%+v", err)
	fmt.Printf("%+v", errors2.Cause(err))
}

func testErr1() error {
	return errors2.Wrapf(errors.New("origin error"), "add testErr1")
}
func testErr2() error {
	return testErr1()
}
func testErr3() error {
	return testErr2()
}
