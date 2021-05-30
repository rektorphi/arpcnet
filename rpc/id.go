package rpc

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"strconv"
)

// ShortID is the part of the ID used in all communication of a connection.
type ShortID = uint64

// IsValidShortID checks if the given number is a valid value for a ShortID.
func IsValidShortID(x ShortID) bool {
	return x > 255
}

// ShortIDString returns a shortened string representation of the ShortID.
func ShortIDString(x ShortID) string {
	return strconv.FormatUint(x&0xffffff, 16)
}

// RandomShortID generated a random ShortID.
func RandomShortID() ShortID {
	bs := make([]byte, 8)
	for {
		_, err := rand.Read(bs)
		if err != nil {
			panic(err)
		}
		res := binary.BigEndian.Uint64(bs)
		if IsValidShortID(res) {
			return ShortID(res)
		}
	}
}

// TokenLength is the length of a token in bytes.
const TokenLength uint = 0

// FullID is the full ID for a connection.
type FullID struct {
	id      ShortID
	source  *Address
	dest    *Address
	token   []byte
	infoStr string
}

func (t *FullID) ID() ShortID {
	return t.id
}

func (t *FullID) Source() *Address {
	return t.source
}

func (t *FullID) Dest() *Address {
	return t.dest
}

func (t *FullID) String() string {
	return t.infoStr
}

// NewFullID create a FullID.
func NewFullID(short ShortID, source *Address, dest *Address, token []byte) *FullID {
	return &FullID{
		id:      short,
		source:  source,
		dest:    dest,
		token:   token,
		infoStr: fmt.Sprintf("%v:%s-%v", source, ShortIDString(short), dest),
	}
}

// RandomFullID creates a random FullID.
func RandomFullID(source *Address, dest *Address) *FullID {
	var bs []byte = make([]byte, TokenLength)
	if TokenLength > 0 {
		_, err := rand.Read(bs)
		if err != nil {
			panic(err)
		}
	}
	return NewFullID(RandomShortID(), source, dest, bs)
}
