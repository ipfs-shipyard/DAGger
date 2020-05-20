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
	Meta ChunkMeta
}
type ChunkMeta map[string]interface{}

func (cm ChunkMeta) Bool(name string) bool {
	if slot, exists := cm[name]; exists {
		if val, isBool := slot.(bool); isBool {
			return val
		}
	}
	return false
}
