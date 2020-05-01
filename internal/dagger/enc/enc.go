package enc

const (
	VarintMaxWireBytes int = 9
)

func AppendVarint(tgt []byte, v uint64) []byte {
	for v > 127 {
		tgt = append(tgt, byte(v|128))
		v >>= 7
	}
	return append(tgt, byte(v))
}

func VarintSlice(v uint64) []byte {
	return AppendVarint(
		make([]byte, 0, VarintMaxWireBytes),
		v,
	)
}
