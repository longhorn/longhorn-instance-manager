package util

import (
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"syscall"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

func RoundUp(num, base uint64) uint64 {
	if num <= 0 {
		return base
	}
	r := num % base
	if r == 0 {
		return num
	}
	return num - r + base
}

const (
	EngineRandomIDLenth = 8
	EngineSuffix        = "-e"
)

func GetVolumeNameFromEngineName(engineName string) string {
	reg := regexp.MustCompile(fmt.Sprintf(`([^"]*)%s-[A-Za-z0-9]{%d,%d}$`, EngineSuffix, EngineRandomIDLenth, EngineRandomIDLenth))
	return reg.ReplaceAllString(engineName, "${1}")
}

func BytesToMiB(bytes uint64) uint64 {
	return bytes / 1024 / 1024
}

func RemovePrefix(path, prefix string) string {
	if strings.HasPrefix(path, prefix) {
		return strings.TrimPrefix(path, prefix)
	}
	return path
}

func UUID() string {
	return uuid.New().String()
}

func IsSPDKTargetProcessRunning() (bool, error) {
	cmd := exec.Command("pgrep", "-f", "spdk_tgt")
	if _, err := cmd.Output(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			status, ok := exitErr.Sys().(syscall.WaitStatus)
			if ok {
				exitCode := status.ExitStatus()
				if exitCode == 1 {
					return false, nil
				}
			}
		}
		return false, errors.Wrap(err, "failed to check spdk_tgt process")
	}
	return true, nil
}

func StartSPDKTgtDaemon() error {
	cmd := exec.Command("spdk_tgt")

	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid: true,
	}

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("failed to start spdk_tgt daemon: %w", err)
	}

	return nil
}

func GetFileChunkChecksum(filePath string, start, size int64) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	if _, err = f.Seek(start, 0); err != nil {
		return "", err
	}

	h := sha512.New()
	if _, err := io.CopyN(h, f, size); err != nil {
		return "", err
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}
