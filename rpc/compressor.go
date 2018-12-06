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
	writerLock  sync.Mutex
	readerLock  sync.Mutex
	gzipWriters []*gzip.Writer
	gzipReaders []*gzip.Reader
}

var _ compressor = (*gzipCompressor)(nil)

func newGzipCompressor() *gzipCompressor {
	return &gzipCompressor{}
}

func (c *gzipCompressor) getGzipWriter(writer io.Writer) (*gzip.Writer, func()) {
	c.writerLock.Lock()
	defer c.writerLock.Unlock()
	var gzipWriter *gzip.Writer
	if len(c.gzipWriters) == 0 {
		gzipWriter = gzip.NewWriter(writer)
	} else {
		gzipWriter = c.gzipWriters[0]
		c.gzipWriters = c.gzipWriters[1:]
		gzipWriter.Reset(writer)
	}
	return gzipWriter, func() {
		c.writerLock.Lock()
		c.gzipWriters = append(c.gzipWriters, gzipWriter)
		c.writerLock.Unlock()
	}
}
func (c *gzipCompressor) getGzipReader(reader io.Reader) (*gzip.Reader, func(), error) {
	c.readerLock.Lock()
	defer c.readerLock.Unlock()
	var err error
	var gzipReader *gzip.Reader
	if len(c.gzipReaders) == 0 {
		if gzipReader, err = gzip.NewReader(reader); err != nil {
			return nil, func() {}, err
		}
	} else {
		gzipReader = c.gzipReaders[0]
		c.gzipReaders = c.gzipReaders[1:]
		gzipReader.Reset(reader)
	}
	return gzipReader, func() {
		c.readerLock.Lock()
		c.gzipReaders = append(c.gzipReaders, gzipReader)
		c.readerLock.Unlock()
	}, nil
}

func (c *gzipCompressor) Compress(data []byte) ([]byte, error) {

	var buf bytes.Buffer
	writer, reclaim := c.getGzipWriter(&buf)
	defer reclaim()

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
	reader, reclaim, err := c.getGzipReader(in)
	if err != nil {
		return nil, err
	}
	defer reclaim()

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
