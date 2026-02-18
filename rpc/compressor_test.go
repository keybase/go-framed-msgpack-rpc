package rpc

import (
	"testing"

	"github.com/keybase/go-codec/codec"
	"github.com/stretchr/testify/require"
)

func MPackEncode(input any) ([]byte, error) {
	mh := codec.MsgpackHandle{WriteExt: true}
	var data []byte
	enc := codec.NewEncoderBytes(&data, &mh)
	if err := enc.Encode(input); err != nil {
		return nil, err
	}
	return data, nil
}

type testData struct {
	Data []byte `codec:"data"`
}

func doWithAllCompressionTypes(fn func(ctype CompressionType)) {
	for _, ctype := range []CompressionType{CompressionGzip, CompressionMsgpackzip} {
		fn(ctype)
	}
}

func TestCompressionAlgs(t *testing.T) {
	doWithAllCompressionTypes(func(ctype CompressionType) {
		c := newCompressorCacher()

		// Make sure we don't make multiple instances of compressors
		compressor := c.getCompressor(ctype)
		c.getCompressor(ctype)
		none := c.getCompressor(CompressionNone)
		require.Nil(t, none)
		require.Len(t, c.algs, 2)

		data, err := MPackEncode(testData{Data: []byte("compress me")})
		require.NoError(t, err)
		zipped, err := compressor.Compress(data)
		require.NoError(t, err)

		unzipped, err := compressor.Decompress(zipped)
		require.NoError(t, err)
		require.Equal(t, data, unzipped)

		garbage, err := compressor.Decompress(data)
		require.Error(t, err)
		require.Nil(t, garbage)

		zipped2, err := compressor.Compress(data)
		require.NoError(t, err)
		unzipped2, err := compressor.Decompress(zipped2)
		require.NoError(t, err)
		require.Equal(t, data, unzipped2)
		require.Equal(t, unzipped, unzipped2)
	})
}
