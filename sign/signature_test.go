package sign

import (
	"net/url"
	"testing"
	"time"
)

const (
	key    = "bearer"
	secret = "W1WTYvJpfeH1YpUjTpeFbEx^DnpQ&35L"
	ttl    = time.Minute * 3
)

func TestSignature(t *testing.T) {
	path := "/echo"
	method := "POST"
	params := url.Values{}
	params.Add("a", "a1")
	params.Add("d", "d1")
	params.Add("c", "c1 c2")

	signatureNew := NewSignature(key, secret, ttl)
	//生成token
	au, date, err := signatureNew.Generate(path, method, params)
	t.Log("authorization:", au)
	t.Log("date:", date)
	t.Log("err:", err)
}
