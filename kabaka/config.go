package kabaka

type Logger interface {
	// 基本日誌級別方法
	Debug(args ...interface{})
	Info(args ...interface{})
	Warn(args ...interface{})
	Error(args ...interface{})
}

type Config struct {
	DataDir string
	Addr    string
	logger  Logger
}
