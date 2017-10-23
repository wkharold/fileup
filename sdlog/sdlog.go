package sdlog

import "cloud.google.com/go/logging"

func LogError(logger *logging.Logger, msg string, err error) {
	logger.Log(logging.Entry{
		Payload: struct {
			Message string
			Error   string
		}{
			Message: msg,
			Error:   err.Error(),
		},
		Severity: logging.Error,
	})
}

func LogInfo(logger *logging.Logger, msg string) {
	logger.Log(logging.Entry{
		Payload: struct {
			Message string
		}{
			Message: msg,
		},
		Severity: logging.Info,
	})
}
