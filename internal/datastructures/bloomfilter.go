package datastructures

import "hash/fnv"

type BloomFilter struct {
	BitArray       []byte
	NumOfHashFuncs uint32
	NumOfBits      uint32
}

func hash1(data []byte) uint32 {
	h := fnv.New32a()
	h.Write(data)
	return h.Sum32()
}

func hash2(data []byte) uint32 {
	h := fnv.New32()
	h.Write(data)
	return h.Sum32()
}

func NewBloomFilterFromSet(numOfHashFuncs, NumOfBits uint32, set map[string]struct{}) *BloomFilter {
	b := NewBloomFilter(numOfHashFuncs, NumOfBits)

	for k := range set {
		for _, pos := range b.getPositions([]byte(k)) {
			byteIndex := pos / 8
			bitIndex := pos % 8
			b.BitArray[byteIndex] |= (1 << bitIndex)
		}
	}

	return b
}

func NewBloomFilter(numOfHashFuncs, NumOfBits uint32) *BloomFilter {
	return &BloomFilter{
		BitArray:       make([]byte, (NumOfBits+7)/8),
		NumOfHashFuncs: numOfHashFuncs,
		NumOfBits:      NumOfBits,
	}
}

func (b *BloomFilter) Contains(key []byte) bool {
	for _, pos := range b.getPositions(key) {
		byteIndex := pos / 8
		bitIndex := pos % 8
		if (b.BitArray[byteIndex] & (1 << bitIndex)) == 0 {
			return false
		}
	}

	return true
}

func (b *BloomFilter) getPositions(key []byte) []uint32 {
	h1 := hash1(key)
	h2 := hash2(key)

	positions := make([]uint32, b.NumOfHashFuncs)
	for i := 0; i < int(b.NumOfHashFuncs); i++ {
		pos := (h1 + uint32(i)*h2) % b.NumOfBits
		positions[i] = pos
	}

	return positions
}

func (b *BloomFilter) ByteSize() int {
	// BitArray Len + NumOfHashFuncs (uint32) + NumOfBits (uint32)
	return len(b.BitArray) + 4 + 4
}
