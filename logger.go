package kabaka

import "sync"

var (
    loggerInstance Logger
    once           sync.Once
)

func setKabakaLogger(logger Logger) {
    once.Do(func() {
        loggerInstance = logger
    })
}

func getKabakaLogger() Logger {
    return loggerInstance
}