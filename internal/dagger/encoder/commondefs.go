package dgrencoder

import (
	dgrblock "github.com/ipfs-shipyard/DAGger/internal/dagger/block"
)

type NodeEncoder interface {
	NewLeaf(leafSource dgrblock.LeafSource) (leafBlock *dgrblock.Header)
	NewLink(origin NodeOrigin, blocksToLink []*dgrblock.Header) (linkBlock *dgrblock.Header)
	IpfsCompatibleNulLink(origin NodeOrigin) (leafBlock *dgrblock.Header)
	LinkframeSize(ofBlock *dgrblock.Header) (bytesNeededToReferenceBlock int)
}

type NodeOrigin struct {
	OriginatorIndex int
	LocalSubLayer   int
}

type Initializer func(
	encoderCLISubArgs []string,
	cfg *DaggerConfig,
) (instance NodeEncoder, initErrorStrings []string)

type DaggerConfig struct {
	GlobalMaxBlockSize   int
	HasherBits           int
	HasherName           string
	BlockMaker           dgrblock.Maker
	NewLinkBlockCallback func(origin NodeOrigin, linkBlock *dgrblock.Header, linkedNodes []*dgrblock.Header)
}
