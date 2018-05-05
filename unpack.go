package tarantool

import (
	"errors"
	"fmt"

	"github.com/tinylib/msgp/msgp"
)

func Unpack(data []byte, fmt string, opts ...interface{}) ([]byte, error) {
	j := uint32(0)
	return unpack(data, fmt, &j, opts...)
}

func UnpackBodyHeader(data []byte) (uint32, []byte, error) {
	var cd uint
	var err error
	var msg string

	_, data, err = msgp.ReadMapHeaderBytes(data)
	if err != nil {
		return 0, data, err
	}

	cd, data, err = msgp.ReadUintBytes(data)
	if err != nil {
		return 0, data, err
	}

	switch cd {
	case KeyError:
		msg, data, err = msgp.ReadStringBytes(data)
		if err != nil {
			return 0, data, err
		}
		return 0, data, errors.New(msg)
	}

	var length uint32
	length, data, err = msgp.ReadArrayHeaderBytes(data)

	return length, data, err
}

func unpack(data []byte, fmt string, pos *uint32, opts ...interface{}) ([]byte, error) {
	var err error

	tupleLen := 0

	for i := 0; i < len(fmt) && err == nil; i++ {
		switch fmt[i] {
		case 'u':
			data, err = unpackUint32(data, opts[*pos])
			*pos++
		case 's':
			data, err = unpackString(data, opts[*pos])
			*pos++
		case '{':
			tupleLen = countTupleLen(fmt[i:])
			data, err = unpackTuple(data, fmt[i:i+tupleLen+1], pos, opts[*pos:*pos+uint32(tupleLen)-1]...)
			i += tupleLen
		case '}':
		case '*':
		}
	}

	return data, err
}

func unpackTuple(data []byte, fmt string, pos *uint32, opts ...interface{}) ([]byte, error) {
	var err error

	var l1 uint32
	l1, data, err = msgp.ReadArrayHeaderBytes(data)
	if err != nil {
		return data, err
	}

	tlen := uint32(countTupleLen(fmt))
	if tlen > l1 {
		fmt = fmt[1 : len(fmt)-int((tlen-l1))]
	}

	for i := uint32(0); i < l1 && err == nil; i++ {
		if i > tlen {
			data, err = msgp.Skip(data)
			continue
		}
		data, err = unpack(data, fmt[*pos:], pos, opts[*pos:]...)
	}

	return data, err
}

func countTupleLen(fmt string) int {
	openedBrackets := 0

	for i := 0; i < len(fmt); i++ {
		if fmt[i] == '}' {
			openedBrackets--
		}
		if fmt[i] == '{' {
			openedBrackets++
		}
		if openedBrackets == 0 {
			return i
		}
	}

	return len(fmt)
}

func unpackString(data []byte, opt interface{}) ([]byte, error) {
	var err error
	switch t := opt.(type) {
	case *string:
		*t, data, err = msgp.ReadStringBytes(data)
	}
	return data, err
}

func unpackUint32(data []byte, opt interface{}) ([]byte, error) {
	var err error
	switch t := opt.(type) {
	case *uint32:
		*t, data, err = msgp.ReadUint32Bytes(data)
	default:
		return data, fmt.Errorf("")
	}
	return data, err
}
