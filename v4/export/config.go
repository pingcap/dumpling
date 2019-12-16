package export

type Config struct {
	// Logger is used to log the export routine.
	Logger Logger
	// Output size limit in bytes.
	OutputSize int
	// OutputDirPath is the directory to output.
	OutputDirPath string
}

const UnspecifiedSize = 0
