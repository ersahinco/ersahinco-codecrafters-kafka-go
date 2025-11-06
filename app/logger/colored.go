package logger

import (
	"fmt"
	"time"
)

const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorCyan   = "\033[36m"
)

func Info(format string, args ...interface{}) {
	timestamp := time.Now().Format("15:04:05")
	fmt.Printf("%s[%s]%s %s\n", colorCyan, timestamp, colorReset, fmt.Sprintf(format, args...))
}

func Success(format string, args ...interface{}) {
	timestamp := time.Now().Format("15:04:05")
	fmt.Printf("%s[%s]%s %s\n", colorGreen, timestamp, colorReset, fmt.Sprintf(format, args...))
}

func Warn(format string, args ...interface{}) {
	timestamp := time.Now().Format("15:04:05")
	fmt.Printf("%s[%s]%s %s\n", colorYellow, timestamp, colorReset, fmt.Sprintf(format, args...))
}

func Error(format string, args ...interface{}) {
	timestamp := time.Now().Format("15:04:05")
	fmt.Printf("%s[%s]%s %s\n", colorRed, timestamp, colorReset, fmt.Sprintf(format, args...))
}

func Debug(format string, args ...interface{}) {
	timestamp := time.Now().Format("15:04:05")
	fmt.Printf("%s[%s]%s %s\n", colorBlue, timestamp, colorReset, fmt.Sprintf(format, args...))
}
