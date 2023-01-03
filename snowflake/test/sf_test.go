package test

import (
	"gitee.com/guolianyu/pkg/snowflake/v3"
	"testing"
)

func TestSF(t *testing.T) {
	workId := snowflake.DefaultWorkId()
	snowflake.NewWorker(int64(workId))
}
