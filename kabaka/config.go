package kabaka

type Logger interface {
	Debug(args ...interface{})
	Info(args ...interface{})
	Warn(args ...interface{})
	Error(args ...interface{})
}

type Config struct {
	logger Logger
}

type Options struct {
	Name       string
	BufferSize int
}

func NewOptions(name string) *Options {
	return &Options{
		Name:       name,
		BufferSize: 24,
	}
}
