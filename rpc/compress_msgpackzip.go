package rpc

import "github.com/keybase/msgpackzip"

type msgpackzipCompressor struct {
	maxDecompressedSize int64
}

var _ compressor = (*msgpackzipCompressor)(nil)

func newMsgpackzipCompressor(maxDecompressedSize int64) *msgpackzipCompressor {
	return &msgpackzipCompressor{maxDecompressedSize: maxDecompressedSize}
}

func (c *msgpackzipCompressor) Compress(data []byte) ([]byte, error) {
	return msgpackzip.Compress(data)
}

func (c *msgpackzipCompressor) Decompress(data []byte) ([]byte, error) {
	return msgpackzip.InflateWithLimit(data, c.maxDecompressedSize)
}
