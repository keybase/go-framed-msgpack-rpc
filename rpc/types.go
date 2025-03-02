package rpc

type DataWrap[H any, D any] struct {
	_struct struct{} `codec:",omitempty"` //lint:ignore U1000 msgpack internal field
	Header  H
	Data    D
}
