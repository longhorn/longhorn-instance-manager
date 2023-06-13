package util

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/google/uuid"
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
