package enc

const (
	PbHdrF1VI = byte(0x08)
	PbHdrF2VI = byte(0x10)
	PbHdrF3VI = byte(0x18)
	PbHdrF4VI = byte(0x20)
	PbHdrF1LD = byte(0x0A)
	PbHdrF2LD = byte(0x12)
	PbHdrF3LD = byte(0x1A)
	PbHdrF4LD = byte(0x22)

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
