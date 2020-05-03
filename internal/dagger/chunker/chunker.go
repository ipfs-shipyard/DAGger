package chunker

type Chunker interface {
	MinChunkSize() (constantSmallestPossibleEmittedChunk int)
	Split(
		rawData []byte,
		moreDataNextRound bool,
		resultCallback func(Chunk),
	)
}

type Chunk struct {
	Size int
	Meta map[interface{}]interface{}
}

type CommonConfig struct {
	GlobalMaxChunkSize int
	InternalPanicf     func(format string, args ...interface{})
}

type Initializer func(
	chunkerCLISubArgs []string,
	cfg *CommonConfig,
) (instance Chunker, initErrorStrings []string)
