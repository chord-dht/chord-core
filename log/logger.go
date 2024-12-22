package log

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// getVerbosity returns the verbosity level from the environment variable VERBOSE.
// If VERBOSE is not set, it returns 0.
// If VERBOSE is set to an invalid value, it logs an error and exits.
//
// 0 - No logging
// 1 - Log info and error messages
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

// Define log file paths.
var (
	infoLogPath  string
	errorLogPath string
	allLogPath   string
	Logger       *CombinedLogger
	titleWidth   = 48
)

type CombinedLogger struct {
	InfoLogger  *log.Logger
	ErrorLogger *log.Logger
	verbosity   int
}

func init() {
	verbosity := getVerbosity()

	if verbosity < 1 {
		Logger = &CombinedLogger{nil, nil, verbosity}
		return
	}

	timestamp := time.Now().Format("20060102_150405")
	infoLogPath = fmt.Sprintf("info_%s.log", timestamp)
	errorLogPath = fmt.Sprintf("error_%s.log", timestamp)
	allLogPath = fmt.Sprintf("all_%s.log", timestamp)

	// Open a file for writing info logs.
	infoLogFile, err := os.OpenFile(infoLogPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Printf("Failed to open info log file: %v", err)
	}

	// Open a file for writing error logs.
	errorLogFile, err := os.OpenFile(errorLogPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Printf("Failed to open error log file: %v", err)
	}

	// Open a file for writing all logs.
	allLogFile, err := os.OpenFile(allLogPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Printf("Failed to open combined log file: %v", err)
	}

	infoWriter := io.MultiWriter(infoLogFile, allLogFile)
	errorWriter := io.MultiWriter(errorLogFile, allLogFile)

	// Create a logger for info messages.
	InfoLogger := log.New(infoWriter, "INFO: ", log.Ldate|log.Ltime|log.Lmicroseconds)

	// Create a logger for error messages.
	ErrorLogger := log.New(errorWriter, "ERROR: ", log.Ldate|log.Ltime|log.Lmicroseconds)

	Logger = &CombinedLogger{InfoLogger, ErrorLogger, verbosity}
}

func (l *CombinedLogger) Info(message string, a ...interface{}) {
	if l.verbosity >= 1 && l.InfoLogger != nil {
		l.InfoLogger.Printf(l.formatLogMessage(message), a...)
	}
}

func Info(message string, a ...interface{}) {
	Logger.Info(message, a...)
}

func (l *CombinedLogger) Error(message string, a ...interface{}) {
	if l.verbosity >= 1 && l.ErrorLogger != nil {
		l.ErrorLogger.Printf(l.formatLogMessage(message), a...)
	}
}

func Error(message string, a ...interface{}) {
	Logger.Error(message, a...)
}

func (l *CombinedLogger) Print(message string, a ...interface{}) {
	if l.verbosity >= 1 {
		fmt.Printf(message+"\n", a...)
	}
}

func Print(message string, a ...interface{}) {
	Logger.Print(message, a...)
}

func (l *CombinedLogger) formatLogMessage(message string) string {
	_, file, line, ok := runtime.Caller(3)
	if !ok {
		file = "???"
		line = 0
	} else {
		file = filepath.Base(file)
	}
	return fmt.Sprintf("%s:%d %s", file, line, message)
}

func CenterTitle(title string, fillChar string) string {
	if len(title) >= titleWidth {
		return title
	}

	padding := (titleWidth - len(title)) / 2
	border := strings.Repeat(fillChar, padding)

	if (titleWidth-len(title))%2 != 0 {
		return fmt.Sprintf("%s%s%s"+fillChar, border, title, border)
	}
	return fmt.Sprintf("%s%s%s", border, title, border)
}

// LogFunctionWithName logs the start and end of a function with the given name.
func LogFunctionWithName(funcName string) func() {
	Logger.Info(CenterTitle(funcName+" start", "=")) // Log the start of the function
	return func() {
		Logger.Info(CenterTitle(funcName+" end", "=")) // Log the end of the function
	}
}

// LogFunction is a helper function to log the start and end of a function
// Use reflect to get the function name
func LogFunction() func() {
	pc, _, _, _ := runtime.Caller(1)
	funcName := runtime.FuncForPC(pc).Name()
	return LogFunctionWithName(funcName)
}

func PrintKeyValue(key string, value interface{}) {
	Logger.Print("%-25s: %v", key, value)
}
