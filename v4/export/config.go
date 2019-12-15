package export

type Config struct {
	// LineSplitter controls the content of splitter when export.
	LineSplitter string
	// Logger is used to log the export routine.
	Logger Logger
	// Output size limit in bytes.
	OutputSize int
	// OutputDirPath is the directory to output.
	OutputDirPath string
}

const UnspecifiedSize = 0
