package rpc

import (
	"bytes"
	"compress/gzip"
	"io"
	"sync"
)

type compressor interface {
	Compress([]byte) ([]byte, error)
	Decompress([]byte) ([]byte, error)
}

type gzipCompressor struct {
	writerLock sync.Mutex
	readerLock sync.Mutex
	gzipWriter *gzip.Writer
	gzipReader *gzip.Reader
}

var _ compressor = (*gzipCompressor)(nil)

func newGzipCompressor() *gzipCompressor {
	return &gzipCompressor{}
}

func (c *gzipCompressor) getGzipWriter(writer io.Writer) *gzip.Writer {
	c.writerLock.Lock()
	defer c.writerLock.Unlock()
	if c.gzipWriter == nil {
		c.gzipWriter = gzip.NewWriter(writer)
		return c.gzipWriter
	}
	c.gzipWriter.Reset(writer)
	return c.gzipWriter
}
func (c *gzipCompressor) getGzipReader(reader io.Reader) (*gzip.Reader, error) {
	c.readerLock.Lock()
	defer c.readerLock.Unlock()
	var err error
	if c.gzipReader == nil {
		if c.gzipReader, err = gzip.NewReader(reader); err != nil {
			c.gzipReader = nil // just make sure
			return nil, err
		}
		return c.gzipReader, nil
	}
	c.gzipReader.Reset(reader)
	return c.gzipReader, nil
}

func (c *gzipCompressor) Compress(data []byte) ([]byte, error) {

	var buf bytes.Buffer
	writer := c.getGzipWriter(&buf)

	if _, err := writer.Write(data); err != nil {
		return nil, err
	}
	if err := writer.Flush(); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c *gzipCompressor) Decompress(data []byte) ([]byte, error) {

	in := bytes.NewBuffer(data)
	reader, err := c.getGzipReader(in)
	if err != nil {
		return nil, err
	}

	var out bytes.Buffer
	if _, err := out.ReadFrom(reader); err != nil {
		return nil, err
	}
	if err := reader.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

type compressorCacher struct {
	sync.Mutex
	algs map[CompressionType]compressor
}

func newCompressorCacher() *compressorCacher {
	return &compressorCacher{
		algs: make(map[CompressionType]compressor),
	}
}

func (c *compressorCacher) getCompressor(ctype CompressionType) compressor {
	c.Lock()
	defer c.Unlock()

	impl, ok := c.algs[ctype]
	if !ok {
		impl = ctype.NewCompressor()
		c.algs[ctype] = impl
	}
	return impl
}
