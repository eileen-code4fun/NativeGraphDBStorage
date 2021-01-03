package graphdb

import (
  "unsafe"
)

type NodeRecord struct {
  nid uint16
  rid uint16
}

func NewNodeRecord(storage []byte) *NodeRecord {
  // Mark in use.
  storage[0] = 1
  return (*NodeRecord)(unsafe.Pointer(&storage[1]))
}

func GetNodeRecord(storage []byte) *NodeRecord {
  if !InUse(storage) {
    return nil
  }
  return (*NodeRecord)(unsafe.Pointer(&storage[1]))
}

func NodeRecordSize() int {
  return int(unsafe.Sizeof(NodeRecord{})) + 1
}

type RelationshipRecord struct {
  rid uint16
  srcID, dstID uint16
  srcRIDPrev, srcRIDNext uint16
  dstRIDPrev, dstRIDNext uint16
}

func NewRelationshipRecord(storage []byte) *RelationshipRecord {
  // Mark in use.
  storage[0] = 1
  return (*RelationshipRecord)(unsafe.Pointer(&storage[1]))
}

func GetRelationshipRecord(storage []byte) *RelationshipRecord {
  if !InUse(storage) {
    return nil
  }
  return (*RelationshipRecord)(unsafe.Pointer(&storage[1]))
}

func RelationshipRecordSize() int {
  return int(unsafe.Sizeof(RelationshipRecord{})) + 1
}

func InUse(storage []byte) bool {
  // Check the first byte.
  return storage[0] == 1
}
