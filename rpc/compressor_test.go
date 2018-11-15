package rpc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGzip(t *testing.T) {
	c := newCompressorCacher()

	// Make sure we don't make multiple instances of compressors
	_ = c.getCompressor(CompressionGzip)
	gz := c.getCompressor(CompressionGzip)
	none := c.getCompressor(CompressionNone)
	require.Nil(t, none)
	require.Len(t, c.algs, 2)

	data := []byte("gzip me")
	zipped, err := gz.Compress(data)
	require.NoError(t, err)

	unzipped, err := gz.Decompress(zipped)
	require.NoError(t, err)
	require.Equal(t, data, unzipped)

	garbage, err := gz.Decompress(data)
	require.Error(t, err)
	require.Nil(t, garbage)

	// compress/decompress again to test reader/writer reuse
	zipped2, err := gz.Compress(data)
	require.NoError(t, err)
	unzipped2, err := gz.Decompress(zipped2)
	require.NoError(t, err)
	require.Equal(t, data, unzipped2)
	require.Equal(t, unzipped, unzipped2)
}
