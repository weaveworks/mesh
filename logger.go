package mesh

import "log"

// Logger is a simple interface used by mesh to do logging.
type Logger interface {
	Printf(format string, args ...interface{})
}

// NewStdlibLogger wraps a stdlib log.Logger.
// If logger is nil, the default stdlib logger is used.
func NewStdlibLogger(logger *log.Logger) Logger {
	return stdlibLogger{logger}
}

type stdlibLogger struct{ logger *log.Logger }

func (l stdlibLogger) Printf(format string, args ...interface{}) {
	if l.logger == nil {
		log.Printf(format, args...)
	} else {
		l.logger.Printf(format, args...)
	}
}
