package chunker

type Chunker interface {
	MinChunkSize() (constantSmallestPossibleEmittedChunk int)
	Split(
		rawDataBuffer []byte,
		useEntireBuffer bool,
		resultCallback SplitResultCallback,
	) error
}

type SplitResultCallback func(Chunk) error

type Chunk struct {
	Size int
	Meta map[interface{}]interface{}
}

type DaggerConfig struct {
	IndexInChain       int
	LastChainIndex     int
	GlobalMaxChunkSize int
	InternalPanicf     func(format string, args ...interface{})
}

type Initializer func(
	chunkerCLISubArgs []string,
	cfg *DaggerConfig,
) (instance Chunker, initErrorStrings []string)
