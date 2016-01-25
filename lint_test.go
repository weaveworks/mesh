package mesh_test

import (
	"os"
	"os/exec"
	"testing"
)

func TestLint(t *testing.T) {
	cmd := exec.Command("./lint")
	// So editors may properly parse file.go:123 prefixes.
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("lint failed: %v", err)
	}
}
