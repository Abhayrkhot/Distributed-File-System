package main

import (
	"os/exec"
	"path/filepath"
	"runtime"
)

func runCmd(name string, args ...string) (string, error) {
	out, err := exec.Command(name, args...).Output()
	return string(out), err
}

// repoBin finds the bin/ directory next to the running binary, or ./bin.
func repoBin() string {
	if _, file, _, ok := runtime.Caller(0); ok {
		return filepath.Join(filepath.Dir(file), "..", "..", "bin")
	}
	return "bin"
}
