package rpc

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"math"
	"sync"
)

var gzipWriterPool = sync.Pool{
	New: func() any {
		return gzip.NewWriter(io.Discard)
	},
}

var gzipReaderPool = sync.Pool{
	New: func() any {
		return new(gzip.Reader)
	},
}

type gzipCompressor struct {
	maxDecompressedSize int64
}

var _ compressor = (*gzipCompressor)(nil)

func newGzipCompressor(maxDecompressedSize int64) *gzipCompressor {
	return &gzipCompressor{maxDecompressedSize: maxDecompressedSize}
}

func (c *gzipCompressor) getGzipWriter(writer io.Writer) (*gzip.Writer, func()) {
	gzipWriter := gzipWriterPool.Get().(*gzip.Writer)
	gzipWriter.Reset(writer)
	return gzipWriter, func() {
		gzipWriterPool.Put(gzipWriter)
	}
}

func (c *gzipCompressor) getGzipReader(reader io.Reader) (*gzip.Reader, func(), error) {
	gzipReader := gzipReaderPool.Get().(*gzip.Reader)
	if err := gzipReader.Reset(reader); err != nil {
		return nil, func() {}, err
	}
	return gzipReader, func() {
		gzipReaderPool.Put(gzipReader)
	}, nil
}

func (c *gzipCompressor) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer, reclaim := c.getGzipWriter(&buf)
	defer reclaim()

	if _, err := writer.Write(data); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c *gzipCompressor) Decompress(data []byte) ([]byte, error) {
	in := bytes.NewReader(data)
	reader, reclaim, err := c.getGzipReader(in)
	if err != nil {
		return nil, err
	}
	// Defers run LIFO: close the reader before returning it to the pool.
	defer reclaim()
	defer reader.Close() //nolint:errcheck

	// +1 sentinel: io.LimitReader yields exactly maxSize+1 bytes when the
	// stream is too large, making the length check below unambiguous.
	// Guard against overflow when maxDecompressedSize is already math.MaxInt64.
	limitCap := c.maxDecompressedSize
	if limitCap < math.MaxInt64 {
		limitCap++
	}
	limited := io.LimitReader(reader, limitCap)
	var out bytes.Buffer
	n, err := out.ReadFrom(limited)
	if err != nil {
		return nil, err
	}
	if n > c.maxDecompressedSize {
		return nil, errors.New("decompressed payload exceeds limit")
	}
	return out.Bytes(), nil
}
