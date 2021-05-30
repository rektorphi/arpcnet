package rpc

import (
	"encoding/binary"
	"fmt"
	"log"
	"reflect"
	"regexp"
	"strings"
	"unsafe"
)

type ByteArray = []byte

// BlankAddress is an address that has no parts.
var BlankAddress = &Address{}

const partSeperator = ':'

var partPattern = regexp.MustCompile(`[\w-_/.]+`)

var addressPattern = regexp.MustCompile(`^[\w-_/.]+(\:[\w-_/.]+)*$`)

// first 2 bytes contains number of parts: n
// then n-1 times 2 bytes indexes of each part after the first
// after n * 2 bytes the first part
type IBList string

func uint16s(b IBList) uint16 {
	_ = b[1] // bounds check hint to compiler; see golang.org/issue/14808
	return uint16(b[0]) | uint16(b[1])<<8
}

func (ib IBList) Get(i int) string {
	n := ib.len()
	ei := uint16(i)
	var sp, ep uint16
	if ei == 0 {
		sp = n * 2
	} else {
		sp = uint16s(ib[2*ei:])
	}
	if ei == n-1 {
		ep = uint16(len(ib))
	} else {
		ep = uint16s(ib[2*(ei+1):]) - 1
	}
	return string(ib[sp:ep])
}

func (ib IBList) len() uint16 {
	if len(ib) == 0 {
		return 0
	}
	return uint16s(ib)
}

func (ib IBList) Len() int {
	return int(ib.len())
}

func NewIBList(bss ...string) (res IBList) {
	n := len(bss)
	if n == 0 {
		return ""
	}
	tlen := 0
	for _, bs := range bss {
		tlen += len(bs)
	}
	resb := make([]byte, (2*n)+tlen+n-1)
	binary.LittleEndian.PutUint16(resb, uint16(n))
	p := 2 * n
	for i, bs := range bss {
		if i > 0 {
			binary.LittleEndian.PutUint16(resb[2*i:], uint16(p))
		}
		copy(resb[p:], bs)
		p += len(bs)
		if i < n-1 {
			resb[p] = ':'
			p++
		}
	}
	return IBList(bytesToString(resb))
}

func (ib IBList) String() string {
	if len(ib) == 0 {
		return ""
	}
	n := ib.Len()
	return string(ib[2*n:])
}

type iblSliceHeader struct {
	ibl IBList
	off int
	len int
}

func newIBLSliceHeader(ibl IBList) iblSliceHeader {
	return iblSliceHeader{ibl, 0, ibl.Len()}
}

func (s iblSliceHeader) forEach(action func(int, string) bool) {
	for i := 0; i < s.len; i++ {
		if !action(i, s.ibl.Get(s.off+i)) {
			break
		}
	}
}

func (s iblSliceHeader) get(i int) string {
	return s.ibl.Get(s.off + i)
}

func (s iblSliceHeader) slice(from, to int) iblSliceHeader {
	if to == -1 {
		to = s.len
	}
	if from > s.len {
		log.Panicf("from %d out of range", from)
	}
	if to > s.len {
		log.Panicf("to %d out of range", to)
	}
	if to-from < 0 {
		panic("from must be smaller than to")
	}
	return iblSliceHeader{s.ibl, s.off + from, to - from}
}

// Address is an immutable data structure of a sequence of segment, like parts in a filesystem path.
// Modifications are cheap operations without any copies by modifying and chaining slice headers. However some operations trigger
// compactification of all slices into a single continous string which can be used for serialization or as a comparable map key.
// The compactification does not change the represented address but is a costly copy so addresses should generally be passed around as pointer so this step is done only once.
// Since an address is immutable, a once compacted address never becomes uncompact again.
type Address struct {
	slices     []iblSliceHeader
	len        int
	compactIBL *IBList
}

func (a Address) Len() int {
	return a.len
}

// Get returns a part of the address.
func (a Address) Get(i int) string {
	if a.compactIBL != nil {
		return a.compactIBL.Get(i)
	}
	c := 0
	for _, s := range a.slices {
		t := c + s.len
		if i >= t {
			c = t
			continue
		}
		return s.get(i - c)
	}
	panic("index out of range")
}

func (a Address) Iter() func() (string, bool) {
	i := 0
	if a.compactIBL != nil {
		return func() (s string, ok bool) {
			if i >= a.compactIBL.Len() {
				return "", false
			}
			s, ok = a.compactIBL.Get(i), true
			i++
			return
		}
	}
	// TODO optimize stepping through slices headers
	return func() (s string, ok bool) {
		if i >= a.len {
			return "", false
		}
		return a.Get(i), ok
	}
}

// Append returns a new address that is a concatenation of this and the other addresses.
// No data is copied, the result is not compact.
func (a Address) Append(other ...*Address) *Address {
	slices := a.slices
	sum := a.len
	for _, o := range other {
		slices = append(slices, o.slices...)
		sum += o.len
	}
	return &Address{slices, sum, nil}
}

// Appends returns a new address that is a concatenation of this and the other strings, each one address part.
// No data is copied, the result is not compact.
func (a Address) Appends(other ...string) *Address {
	ibl := NewIBList(other...)
	return &Address{append(a.slices, newIBLSliceHeader(ibl)), a.len + len(other), nil}
}

// Slice returns a new address that is a sub list of this address. A 'to'-value of -1 means till the end.
/// No data is copied, the result is not compact.
func (a Address) Slice(from, to int) *Address {
	if from < 0 || from > a.len {
		log.Panicf("from %d out of range", from)
	}
	if to == -1 {
		to = a.len
	} else if to > a.len {
		log.Panicf("to %d out of range", to)
	}
	if to-from <= 0 {
		return BlankAddress
	}
	if a.compactIBL != nil {
		return &Address{[]iblSliceHeader{newIBLSliceHeader(*a.compactIBL).slice(from, to)}, to - from, nil}
	}
	beginSlice := 0
	sum := 0
	beginSliceSum := 0
	for i, s := range a.slices {
		t := sum + s.len
		if from >= t {
			sum = t
			continue
		}
		beginSlice = i
		beginSliceSum = sum
		break
	}
	endSlice := beginSlice
	endSliceSum := beginSliceSum
	for i := beginSlice; i < len(a.slices); i++ {
		t := sum + a.slices[i].len
		if to > t {
			sum = t
			continue
		}
		endSlice = i
		endSliceSum = sum
		break
	}
	if beginSlice == endSlice {
		return &Address{[]iblSliceHeader{a.slices[beginSlice].slice(from-beginSliceSum, to-beginSliceSum)}, to - from, nil}
	}
	resSlice := make([]iblSliceHeader, endSlice+1-beginSlice)
	copy(resSlice, a.slices[beginSlice:endSlice+1])
	resSlice[0] = resSlice[0].slice(from-beginSliceSum, -1)
	resSlice[len(resSlice)-1].slice(0, to-endSliceSum)
	return &Address{resSlice, to - from, nil}
}

// Compact optimizes the data representation of this address into a single contigous string.
// Is a no-op if this address is already compact.
func (a *Address) Compact() *IBList {
	if a.compactIBL == nil {
		parts := make([]string, 0, a.len)
		for _, s := range a.slices {
			s.forEach(func(i int, s string) bool {
				parts = append(parts, s)
				return true
			})
		}
		ibl := NewIBList(parts...)
		a.slices = []iblSliceHeader{newIBLSliceHeader(ibl)}
		a.compactIBL = &a.slices[0].ibl
	}
	return a.compactIBL
}

// String returns a string of the contents of this address with an arbitrary seperation character.
// Note that the address may not be a legal UTF-8 string and can contain arbitrary byte sequences.
// This operation compacts the address and once compacted, it is quick without further copies or string construction.
func (a *Address) String() string {
	return a.Compact().String()
}

// NewAddress creates a new address from the given strings, each one part of the address sequence.
// The resulting address is compact.
func NewAddress(parts ...string) *Address {
	ibl := NewIBList(parts...)
	res := &Address{[]iblSliceHeader{newIBLSliceHeader(ibl)}, ibl.Len(), nil}
	res.compactIBL = &res.slices[0].ibl
	return res
}

// Data returns the raw string representation of this address.
// An address can be reconstructed from this string quickly without parsing.
// This operation compacts the address and once compacted, it is quick without further copies or string construction.
func (a *Address) Data() string {
	return string(*a.Compact())
}

// NewAddressFromData creates a new address from the given data string which must be the result from Address.Data().
// The resulting address is compact and this operation is fast.
func NewAddressFromData(s string) *Address {
	ibl := IBList(s)
	res := &Address{[]iblSliceHeader{newIBLSliceHeader(ibl)}, ibl.Len(), nil}
	res.compactIBL = &res.slices[0].ibl
	return res
}

// This operation compacts both addresses..
func (a *Address) Compare(other *Address) int {
	minl := a.len
	if other.len < minl {
		minl = other.len
	}
	// TODO optimize using iter
	return strings.Compare(a.Compact().String(), other.Compact().String())
}

func (a Address) Equal(other *Address) bool {
	if a.len != other.len {
		return false
	}
	// TODO optimize using iter
	for i := 0; i < a.len; i++ {
		if a.Get(i) != other.Get(i) {
			return false
		}
	}
	return true
}

func (a Address) StartsWith(other *Address) bool {
	if other.len > a.len {
		return false
	}
	aiter := a.Iter()
	oiter := other.Iter()
	for {
		as, aok := aiter()
		os, ook := oiter()
		if !ook {
			return !aok
		}
		if as != os {
			return false
		}
	}
}

func (a Address) ToProto() []byte {
	return stringToBytes(a.Data())
}

func AddressFromProto(ap []byte) *Address {
	return NewAddressFromData(bytesToString(ap))
}

// MustParseAddress same as ParseAddress but panics on error.
func MustParseAddress(s string) *Address {
	res, err := ParseAddress(s)
	if err != nil {
		panic(err)
	}
	return res
}

// ParseAddress parses a string into an address.
func ParseAddress(s string) (*Address, error) {
	if len(s) == 0 {
		return BlankAddress, nil
	}
	if !addressPattern.MatchString(s) {
		return BlankAddress, fmt.Errorf("%s is not a valid address", s)
	}
	res := partPattern.FindAllString(s, -1)
	return NewAddress(res...), nil
}

func bytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func stringToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&s))
}

func bytesToString2(b []byte) string {
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	sh := reflect.StringHeader{Data: bh.Data, Len: bh.Len}
	return *(*string)(unsafe.Pointer(&sh))
}

func stringToBytes2(s string) []byte {
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := reflect.SliceHeader{Data: sh.Data, Len: sh.Len, Cap: sh.Len}
	return *(*[]byte)(unsafe.Pointer(&bh))
}
