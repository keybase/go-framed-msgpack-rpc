package rpc

import (
	"sync"
)

// decompressedSizeMultiplier caps decompressed output at this multiple of
// maxFrameLength to prevent decompression-bomb DoS from a malicious peer.
// 50× yields a 5 GiB ceiling for the 100 MiB default frame length, which
// is generous enough to avoid false positives for high-ratio payloads while
// still bounding worst-case allocation. Adjust DefaultMaxFrameLength to
// scale the decompression cap proportionally.
const decompressedSizeMultiplier int64 = 50

type compressor interface {
	Compress([]byte) ([]byte, error)
	Decompress([]byte) ([]byte, error)
}

type compressorCacher struct {
	sync.Mutex
	maxDecompressedSize int64
	algs                map[CompressionType]compressor
}

func newCompressorCacher(maxDecompressedSize int64) *compressorCacher {
	return &compressorCacher{
		maxDecompressedSize: maxDecompressedSize,
		algs:                make(map[CompressionType]compressor),
	}
}

func (c *compressorCacher) getCompressor(ctype CompressionType) compressor {
	c.Lock()
	defer c.Unlock()

	impl, ok := c.algs[ctype]
	if !ok {
		impl = ctype.NewCompressor(c.maxDecompressedSize)
		c.algs[ctype] = impl
	}
	return impl
}
