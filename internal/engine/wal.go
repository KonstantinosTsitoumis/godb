package engine

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

type WAL struct {
	file *os.File
}

const wALFileName = "WAL.log"

func NewWAL(path string) (*WAL, error) {
	f, err := getWalFile(filepath.Join(path, wALFileName))
	if err != nil {
		return nil, fmt.Errorf("get wal file: %w", err)
	}

	return &WAL{file: f}, nil
}

func (w WAL) Close() error {
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("file close: %w", err)
	}

	return nil
}

func (w WAL) Append(op OpType, key, value []byte) error {
	entry := w.encodeRecord(byte(op), key, value)

	if _, err := w.file.Write(entry); err != nil {
		return fmt.Errorf("file write: %w", err)
	}

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}

	return nil
}

const (
	opBytes     = 1
	lengthBytes = uint32Bytes
	keyLenBytes = uint32Bytes
	valLenBytes = uint32Bytes
	crc32Bytes  = uint32Bytes
)

func (w WAL) encodeRecord(op byte, key, value []byte) []byte {
	keyLen := len(key)
	valLen := len(value)

	payload := make([]byte, 0, opBytes+keyLenBytes+valLenBytes+keyLen+valLen)
	payload = append(payload, op)
	payload = binary.BigEndian.AppendUint32(payload, uint32(keyLen))
	payload = binary.BigEndian.AppendUint32(payload, uint32(valLen))
	payload = append(payload, key...)
	payload = append(payload, value...)

	crc32 := crc32.ChecksumIEEE(payload)
	record := make([]byte, 0, len(payload)+crc32Bytes)
	record = append(record, payload...)
	record = binary.BigEndian.AppendUint32(record, crc32)

	length := len(record)
	entry := make([]byte, 0, len(record)+lengthBytes)
	entry = binary.BigEndian.AppendUint32(entry, uint32(length))
	entry = append(entry, record...)

	return entry
}

type WALEntry struct {
	Op     OpType
	KeyLen uint32
	ValLen uint32
	Key    []byte
	Value  []byte
	Crc32  uint32
}

type OpType byte

const (
	WALDEL OpType = 0
	WALPUT OpType = 1
)

func (w *WAL) Load() ([]WALEntry, error) {
	var result []WALEntry

	for {
		lengthBuf := make([]byte, lengthBytes)
		_, err := io.ReadFull(w.file, lengthBuf)
		if err == io.EOF {
			return result, nil
		}
		if err != nil {
			return nil, err
		}

		length := binary.BigEndian.Uint32(lengthBuf)
		if length == 0 {
			return nil, errors.New("zero-length WAL record")
		}

		record := make([]byte, length)
		_, err = io.ReadFull(w.file, record)
		if err != nil {
			return nil, err
		}

		entry, err := decodeRecord(record)
		if err != nil {
			return nil, err
		}

		result = append(result, entry)
	}
}

func decodeRecord(buf []byte) (WALEntry, error) {
	if len(buf) < opBytes+keyLenBytes+valLenBytes+crc32Bytes {
		return WALEntry{}, errors.New("record too short")
	}

	payloadLen := len(buf) - crc32Bytes
	payload := buf[:payloadLen]

	expectedCRC := binary.BigEndian.Uint32(buf[payloadLen:])
	actualCRC := crc32.ChecksumIEEE(payload)

	if expectedCRC != actualCRC {
		return WALEntry{}, errors.New("crc32 mismatch")
	}

	var off uint32 = 0

	op := payload[off]
	off += opBytes

	keyLen := binary.BigEndian.Uint32(payload[off : off+keyLenBytes])
	off += keyLenBytes

	valLen := binary.BigEndian.Uint32(payload[off : off+valLenBytes])
	off += valLenBytes

	if int(off+keyLen+valLen) != payloadLen {
		return WALEntry{}, errors.New("length mismatch")
	}

	key := payload[off : off+keyLen]
	off += keyLen

	value := payload[off : off+valLen]

	return WALEntry{
		Op:     OpType(op),
		KeyLen: keyLen,
		ValLen: valLen,
		Key:    key,
		Value:  value,
		Crc32:  expectedCRC,
	}, nil
}

func getWalFile(path string) (*os.File, error) {
	flag := os.O_RDWR | os.O_APPEND | os.O_CREATE
	file, err := os.OpenFile(path, flag, 0644)
	if err != nil {
		return &os.File{}, fmt.Errorf("open file: %w", err)
	}

	return file, nil
}
