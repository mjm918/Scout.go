package log

import (
	"Scout.go/util"
	accesslog "github.com/mash/go-accesslog"
	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"strconv"
)

var L = NewLogger("DEBUG", util.LogPath(), 10, 10, 1, false)

func NewLogger(logLevel string, logFilename string, logMaxSize int, logMaxBackups int, logMaxAge int, logCompress bool) *zap.Logger {
	var ll zapcore.Level
	switch logLevel {
	case "DEBUG":
		ll = zap.DebugLevel
	case "INFO":
		ll = zap.InfoLevel
	case "WARN", "WARNING":
		ll = zap.WarnLevel
	case "ERR", "ERROR":
		ll = zap.WarnLevel
	case "DPANIC":
		ll = zap.DPanicLevel
	case "PANIC":
		ll = zap.PanicLevel
	case "FATAL":
		ll = zap.FatalLevel
	}

	var ws zapcore.WriteSyncer
	switch logFilename {
	case "", os.Stderr.Name():
		ws = zapcore.AddSync(os.Stderr)
	case os.Stdout.Name():
		ws = zapcore.AddSync(os.Stdout)
	default:
		ws = zapcore.AddSync(
			&lumberjack.Logger{
				Filename:   logFilename,
				MaxSize:    logMaxSize, // megabytes
				MaxBackups: logMaxBackups,
				MaxAge:     logMaxAge, // days
				Compress:   logCompress,
			},
		)
	}

	ec := zap.NewProductionEncoderConfig()
	ec.TimeKey = "_timestamp_"
	ec.LevelKey = "_level_"
	ec.NameKey = "_name_"
	ec.CallerKey = "_caller_"
	ec.MessageKey = "_message_"
	ec.StacktraceKey = "_stacktrace_"
	ec.EncodeTime = zapcore.ISO8601TimeEncoder
	ec.EncodeCaller = zapcore.ShortCallerEncoder

	logger := zap.New(
		zapcore.NewCore(
			zapcore.NewJSONEncoder(ec),
			ws,
			ll,
		),
		zap.AddCaller(),
		//zap.AddStacktrace(ll),
	).Named(os.Getenv("APPNAME"))

	return logger
}

type HTTPLogger struct {
	Logger *zap.Logger
}

func (l HTTPLogger) Log(record accesslog.LogRecord) {
	// Output log that formatted Apache combined.
	size := "-"
	if record.Size > 0 {
		size = strconv.FormatInt(record.Size, 10)
	}

	referer := "-"
	if record.RequestHeader.Get("Referer") != "" {
		referer = record.RequestHeader.Get("Referer")
	}

	userAgent := "-"
	if record.RequestHeader.Get("User-Agent") != "" {
		userAgent = record.RequestHeader.Get("User-Agent")
	}
	//time.LoadLocation("A")
	l.Logger.Info(
		"",
		zap.String("ip", record.Ip),
		zap.String("username", record.Username),
		zap.String("time", record.Time.Format("02/Jan/2006 03:04:05 +0000")),
		zap.String("method", record.Method),
		zap.String("uri", record.Uri),
		zap.String("protocol", record.Protocol),
		zap.Int("status", record.Status),
		zap.String("size", size),
		zap.String("referer", referer),
		zap.String("user_agent", userAgent),
	)
}

var CL = NewCanalLogger()

type CanalLogger struct {
	Logger *zap.Logger
}

func (c *CanalLogger) Fatal(args ...interface{}) {
	c.Logger.Fatal("canal", zap.Any("args", args))
}

func (c *CanalLogger) Fatalf(format string, args ...interface{}) {
	c.Logger.Fatal("canal", zap.Any("args", args))
}

func (c *CanalLogger) Fatalln(args ...interface{}) {
	c.Logger.Fatal("canal", zap.Any("args", args))
}

func (c *CanalLogger) Panic(args ...interface{}) {
	c.Logger.Fatal("canal", zap.Any("args", args))
}

func (c *CanalLogger) Panicf(format string, args ...interface{}) {
	c.Logger.Fatal("canal", zap.Any("args", args))
}

func (c *CanalLogger) Panicln(args ...interface{}) {
	c.Logger.Fatal("canal", zap.Any("args", args))
}

func (c *CanalLogger) Print(args ...interface{}) {
	c.Logger.Info("canal", zap.Any("args", args))
}

func (c *CanalLogger) Printf(format string, args ...interface{}) {
	c.Logger.Info("canal", zap.Any("args", args))
}

func (c *CanalLogger) Println(args ...interface{}) {
	c.Logger.Info("canal", zap.Any("args", args))
}

func (c *CanalLogger) Debug(args ...interface{}) {
	c.Logger.Debug("canal", zap.Any("args", args))
}

func (c *CanalLogger) Debugf(format string, args ...interface{}) {
	c.Logger.Debug("canal", zap.Any("args", args))
}

func (c *CanalLogger) Debugln(args ...interface{}) {
	c.Logger.Debug("canal", zap.Any("args", args))
}

func (c *CanalLogger) Error(args ...interface{}) {
	c.Logger.Error("canal", zap.Any("args", args))
}

func (c *CanalLogger) Errorf(format string, args ...interface{}) {
	c.Logger.Error("canal", zap.Any("args", args))
}

func (c *CanalLogger) Errorln(args ...interface{}) {
	c.Logger.Error("canal", zap.Any("args", args))
}

func (c *CanalLogger) Info(args ...interface{}) {
	c.Logger.Info("canal", zap.Any("args", args))
}

func (c *CanalLogger) Infof(format string, args ...interface{}) {
	c.Logger.Info("canal", zap.Any("args", args))
}

func (c *CanalLogger) Infoln(args ...interface{}) {
	c.Logger.Info("canal", zap.Any("args", args))
}

func (c *CanalLogger) Warn(args ...interface{}) {
	c.Logger.Warn("canal", zap.Any("args", args))
}

func (c *CanalLogger) Warnf(format string, args ...interface{}) {
	c.Logger.Warn("canal", zap.Any("args", args))
}

func (c *CanalLogger) Warnln(args ...interface{}) {
	c.Logger.Warn("canal", zap.Any("args", args))
}

func NewCanalLogger() *CanalLogger {
	return &CanalLogger{
		Logger: NewLogger("DEBUG", util.CanalLogPath(), 10, 10, 1, false),
	}
}
