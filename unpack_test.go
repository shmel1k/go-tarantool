package tarantool

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnpack(t *testing.T) {
	assert := assert.New(t)

	tarantoolConfig := `
    s = box.schema.space.create('tester', {id = 42})
    s:create_index('tester_id', {
        type = 'hash',
        parts = {1, 'NUM'}
    })
	s:create_index('tester_name', {
        type = 'hash',
        parts = {2, 'STR'}
    })
	s:create_index('id_name', {
        type = 'hash',
        parts = {1, 'NUM', 2, 'STR'},
        unique = true
    })
    t = s:insert({1, 'First record'})
    t = s:insert({2, 'Music'})
    t = s:insert({3, 'Length', 93})
    
    function sel_all()
        return box.space.tester:select{}
    end

    function sel_name(tester_id, name)
        return box.space.tester.index.id_name:select{tester_id, name}
    end

    box.schema.func.create('sel_all', {if_not_exists = true})
    box.schema.func.create('sel_name', {if_not_exists = true})

    box.schema.user.grant('guest', 'execute', 'function', 'sel_all', {if_not_exists = true})
    box.schema.user.grant('guest', 'execute', 'function', 'sel_name', {if_not_exists = true})    
    `

	type someStruct struct {
		fieldOne   uint32
		fieldTwo   string
		fieldThree uint32
	}

	box, err := NewBox(tarantoolConfig, nil)
	if !assert.NoError(err) {
		return
	}
	defer box.Close()

	do := func(connectOptions *Options, query *Call, expected []someStruct) {
		var buf []byte

		conn, err := box.Connect(connectOptions)
		assert.NoError(err)
		assert.NotNil(conn)

		defer conn.Close()

		buf, err = query.PackMsg(conn.packData, buf)

		if assert.NoError(err) {
			var query2 = &Call{}
			err = query2.UnmarshalBinary(buf)

			if assert.NoError(err) {
				assert.Equal(query.Name, query2.Name)
				assert.Equal(query.Tuple, query2.Tuple)
			}
		}

		data, _, err := conn.ExecuteRaw(context.Background(), query)
		if assert.NoError(err) {
			data.body, err = data.packet.UnmarshalBinaryHeader(data.body)

			var l uint32
			var res []someStruct
			var length uint32
			length, data.body, err = UnpackBodyHeader(data.body)
			assert.NoError(err)
			res = make([]someStruct, 0, l)
			var r someStruct
			for i := uint32(0); i < length; i++ {
				assert.NoError(err)
				data.body, err = Unpack(data.body, "{usu}", &r.fieldOne, &r.fieldTwo, &r.fieldThree)
				assert.NoError(err)
				res = append(res, r)
			}
			assert.Equal(expected, res)
		}
	}

	// call sel_all without params
	do(nil,
		&Call{
			Name: "sel_all",
		},
		[]someStruct{
			someStruct{
				fieldOne: uint32(1),
				fieldTwo: "First record",
			},
			someStruct{
				fieldOne: uint32(2),
				fieldTwo: "Music",
			},
			someStruct{
				fieldOne:   uint32(3),
				fieldTwo:   "Length",
				fieldThree: uint32(93),
			},
		},
	)
}
