package dgrencoder

import (
	dgrblock "github.com/ipfs-shipyard/DAGger/internal/dagger/block"
)

type NodeEncoder interface {
	NewLeaf(leafSource dgrblock.DataSource) (leafBlock *dgrblock.Header)
	NewLink(origin NodeOrigin, blocksToLink []*dgrblock.Header) (linkBlock *dgrblock.Header)
}

type NodeOrigin struct {
	OriginatingLayer int
	LocalSubLayer    int
}

type Initializer func(
	encoderCLISubArgs []string,
	cfg *DaggerConfig,
) (instance NodeEncoder, initErrorStrings []string)

type DaggerConfig struct {
	HasherBits           int
	HasherName           string
	BlockMaker           dgrblock.Maker
	NewLinkBlockCallback func(origin NodeOrigin, block *dgrblock.Header, linkedNodes []*dgrblock.Header)
}
