package mesh

import (
	"bytes"
	"log"
	"testing"
)

func TestStdlibLoggerNil(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	log.SetFlags(0)
	NewStdlibLogger(nil).Printf("hello %s", "world")
	if want, have := "hello world\n", buf.String(); want != have {
		t.Errorf("want %q, have %q", want, have)
	}
}

func TestStdlibLogger(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "> ", 0)
	NewStdlibLogger(logger).Printf("123 %d", 456)
	if want, have := "> 123 456\n", buf.String(); want != have {
		t.Errorf("want %q, have %q", want, have)
	}
}
