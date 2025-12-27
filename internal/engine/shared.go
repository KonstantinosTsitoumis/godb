package engine

var (
	tombstone    = []byte("__TOMBSTONE__")
	tombstoneLen = uint32(len(tombstone))
)

const (
	uint32Bytes          = 4
	DBMagicNumber uint32 = 1337
)
