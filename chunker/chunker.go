package chunker

type Chunker interface {
	Split(
		rawDataBuffer []byte,
		useEntireBuffer bool,
		resultCallback SplitResultCallback,
	) error
}

type SplitResultCallback func(
	singleChunkingResult Chunk,
) error

type Chunk struct {
	Size int
	Meta map[string]interface{}
}
