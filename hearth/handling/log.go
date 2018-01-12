package handling

type Logger interface {

// Started uses the Serialize destination and adds a Started tag to the log line
   Started(title string, functionName string)
// Startedf uses the Serialize destination and writes a Started tag to the log line
   Startedf(title string, functionName string, format string, a ...interface{})

// Completed uses the Serialize destination and writes a Completed tag to the log line
   Completed(title string, functionName string)
   Completedf(title string, functionName string, format string, a ...interface{})
   CompletedError(err error, title string, functionName string)
   CompletedErrorf(err error, title string, functionName string, format string, a ...interface{})

//** TRACE

// Trace writes to the Trace destination
 Trace(title string, functionName string, format string, a ...interface{})

//** INFO
// Info writes to the Info destination
 Info(title string, functionName string, format string, a ...interface{})

//** WARNING
// Warning writes to the Warning destination
 Warning(title string, functionName string, format string, a ...interface{})

//** ERROR
// Error writes to the Error destination and accepts an err
 Error(err error, title string, functionName string)

// Errorf writes to the Error destination and accepts an err
 Errorf(err error, title string, functionName string, format string, a ...interface{})
//** ALERT
// Alert write to the Error destination and sends email alert
 Alert(subject string, title string, functionName string, format string, a ...interface{})
// CompletedAlert write to the Error destination, writes a Completed tag to the log line and sends email alert
 CompletedAlert(subject string, title string, functionName string, format string, a ...interface{})


}