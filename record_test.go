package graphdb

// go test -v -run Test*

import (
  "reflect"
  "testing"
)

func TestNodeRecord(t *testing.T) {
  storage := make([]byte, 3 * nodeRecordSize)
  for i := 0; i < 3 * nodeRecordSize; i ++ {
    storage[i] = 0
  }
  n1 := NodeRecord{
    nid: 1,
    rid: 2,
  }
  n2 := NodeRecord{
    nid: 11,
    rid: 12,
  }
  MarshalNode(n1, storage)
  MarshalNode(n2, storage[2*nodeRecordSize:])
  want := []*NodeRecord{&n1, nil, &n2}
  got := []*NodeRecord{UnmarshalNode(storage)}
  got = append(got, UnmarshalNode(storage[nodeRecordSize:]))
  got = append(got, UnmarshalNode(storage[2*nodeRecordSize:]))
  if !reflect.DeepEqual(got, want) {
    t.Errorf("TestNodeRecord got %v; want %v", got, want)
  }
}

func TestRelationshipRecord(t *testing.T) {
  storage := make([]byte, 3 * relationshipRecordSize)
  for i := 0; i < 3 * relationshipRecordSize; i ++ {
    storage[i] = 0
  }
  r1 := RelationshipRecord{
    rid: 1,
    srcID: 2,
    dstID: 3,
    srcRIDPrev: 4,
    srcRIDNext: 5,
    dstRIDPrev: 6,
    dstRIDNext: 7,
  }
  r2 := RelationshipRecord{
    rid: 11,
    srcID: 12,
    dstID: 13,
    srcRIDPrev: 14,
    srcRIDNext: 15,
    dstRIDPrev: 16,
    dstRIDNext: 17,
  }
  MarshalRelationship(r1, storage)
  MarshalRelationship(r2, storage[2*relationshipRecordSize:])
  want := []*RelationshipRecord{&r1, nil, &r2}
  got := []*RelationshipRecord{UnmarshalRelationship(storage)}
  got = append(got, UnmarshalRelationship(storage[relationshipRecordSize:]))
  got = append(got, UnmarshalRelationship(storage[2*relationshipRecordSize:]))
  if !reflect.DeepEqual(got, want) {
    t.Errorf("TestRelationshipRecord got %v; want %v", got, want)
  }
}
