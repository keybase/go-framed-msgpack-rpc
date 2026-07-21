package rpc

import (
	"bytes"
	"errors"
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

// compressiblePayload returns msgpack-encoded data that compresses well.
func compressiblePayload(t *testing.T, size int) []byte {
	t.Helper()
	data, err := MPackEncode(testData{Data: bytes.Repeat([]byte("a"), size)})
	require.NoError(t, err)
	return data
}

// TestDecompressionLimit verifies that both compressors enforce the
// maxDecompressedSize limit and reject payloads that would exceed it.
func TestDecompressionLimit(t *testing.T) {
	doWithAllCompressionTypes(func(ctype CompressionType) {
		t.Run(ctype.String(), func(t *testing.T) {
			payload := compressiblePayload(t, 10_000)

			// Compress with a generous limit so we can produce a valid payload.
			generous := newCompressorCacher(int64(DefaultMaxFrameLength) * decompressedSizeMultiplier)
			compressor := generous.getCompressor(ctype)
			compressed, err := compressor.Compress(payload)
			require.NoError(t, err)

			decompressedLen := int64(len(payload))

			t.Run("within_limit", func(t *testing.T) {
				c := ctype.NewCompressor(decompressedLen + 1)
				out, err := c.Decompress(compressed)
				require.NoError(t, err)
				require.Equal(t, payload, out)
			})

			t.Run("at_exact_limit", func(t *testing.T) {
				c := ctype.NewCompressor(decompressedLen)
				out, err := c.Decompress(compressed)
				require.NoError(t, err)
				require.Equal(t, payload, out)
			})

			t.Run("one_byte_under_limit", func(t *testing.T) {
				c := ctype.NewCompressor(decompressedLen - 1)
				_, err := c.Decompress(compressed)
				require.Error(t, err)
				require.Contains(t, err.Error(), "exceeds limit")
			})

			t.Run("very_small_limit", func(t *testing.T) {
				c := ctype.NewCompressor(1)
				_, err := c.Decompress(compressed)
				require.Error(t, err)
				require.Contains(t, err.Error(), "exceeds limit")
			})
		})
	})
}

func TestGzipConcurrentDecompress(t *testing.T) {
	payload := compressiblePayload(t, 10_000)
	compressor := newGzipCompressor(int64(len(payload)))
	compressed, err := compressor.Compress(payload)
	require.NoError(t, err)

	const (
		workers    = 16
		iterations = 100
	)
	start := make(chan struct{})
	results := make(chan error, workers)
	for range workers {
		go func() {
			<-start
			for range iterations {
				decompressed, err := compressor.Decompress(compressed)
				if err != nil {
					results <- err
					return
				}
				if !bytes.Equal(payload, decompressed) {
					results <- errors.New("decompressed payload does not match input")
					return
				}
			}
			results <- nil
		}()
	}
	close(start)

	for range workers {
		require.NoError(t, <-results)
	}
}

func TestCompressionAlgs(t *testing.T) {
	doWithAllCompressionTypes(func(ctype CompressionType) {
		c := newCompressorCacher(int64(DefaultMaxFrameLength) * decompressedSizeMultiplier)

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
