package graphdb

import (
  "encoding/binary"
)

const (
  nodeRecordSize = 4 + 1
  relationshipRecordSize = 14 + 1
)

type NodeRecord struct {
  nid uint16
  rid uint16
}

func MarshalNode(n NodeRecord, storage []byte) {
  // Mark in use.
  storage[0] = 1
  binary.LittleEndian.PutUint16(storage[1:], n.nid)
  binary.LittleEndian.PutUint16(storage[3:], n.rid)
}

func UnmarshalNode(storage []byte) *NodeRecord {
  if !InUse(storage) {
    return nil
  }
  n := &NodeRecord{}
  n.nid = binary.LittleEndian.Uint16(storage[1:])
  n.rid = binary.LittleEndian.Uint16(storage[3:])
  return n
}

type RelationshipRecord struct {
  rid uint16
  srcID, dstID uint16
  srcRIDPrev, srcRIDNext uint16
  dstRIDPrev, dstRIDNext uint16
}

func MarshalRelationship(r RelationshipRecord, storage []byte) {
  storage[0] = 1
  binary.LittleEndian.PutUint16(storage[1:], r.rid)
  binary.LittleEndian.PutUint16(storage[3:], r.srcID)
  binary.LittleEndian.PutUint16(storage[5:], r.dstID)
  binary.LittleEndian.PutUint16(storage[7:], r.srcRIDPrev)
  binary.LittleEndian.PutUint16(storage[9:], r.srcRIDNext)
  binary.LittleEndian.PutUint16(storage[11:], r.dstRIDPrev)
  binary.LittleEndian.PutUint16(storage[13:], r.dstRIDNext)
}

func UnmarshalRelationship(storage []byte) *RelationshipRecord {
  if !InUse(storage) {
    return nil
  }
  r := &RelationshipRecord{}
  r.rid = binary.LittleEndian.Uint16(storage[1:])
  r.srcID = binary.LittleEndian.Uint16(storage[3:])
  r.dstID = binary.LittleEndian.Uint16(storage[5:])
  r.srcRIDPrev = binary.LittleEndian.Uint16(storage[7:])
  r.srcRIDNext = binary.LittleEndian.Uint16(storage[9:])
  r.dstRIDPrev = binary.LittleEndian.Uint16(storage[11:])
  r.dstRIDNext = binary.LittleEndian.Uint16(storage[13:])
  return r
}

func InUse(storage []byte) bool {
  // Check the first byte.
  return storage[0] == 1
}
