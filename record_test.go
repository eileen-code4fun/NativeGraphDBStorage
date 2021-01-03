package graphdb

// go test -v -run Test*

import (
  "reflect"
  "testing"
)

func TestNodeRecord(t *testing.T) {
  storage := make([]byte, 3 * NodeRecordSize())
  for i := 0; i < 3 * NodeRecordSize(); i ++ {
    storage[i] = 0
  }
  n1 := NewNodeRecord(storage)
  n1.nid = 1
  n1.rid = 2
  n2 := NewNodeRecord(storage[2*NodeRecordSize():])
  n2.nid = 11
  n2.rid = 12
  want := []*NodeRecord{n1, nil, n2}
  got := []*NodeRecord{GetNodeRecord(storage)}
  got = append(got, GetNodeRecord(storage[NodeRecordSize():]))
  got = append(got, GetNodeRecord(storage[2*NodeRecordSize():]))
  if !reflect.DeepEqual(got, want) {
    t.Errorf("TestNodeRecord got %v; want %v", got, want)
  }
}

func TestRelationshipRecord(t *testing.T) {
  storage := make([]byte, 3 * RelationshipRecordSize())
  for i := 0; i < 3 * RelationshipRecordSize(); i ++ {
    storage[i] = 0
  }
  r1 := NewRelationshipRecord(storage)
  r1.rid = 1
  r1.srcID = 2
  r1.dstID = 3
  r1.srcRIDPrev = 4
  r1.srcRIDNext = 5
  r1.dstRIDPrev = 6
  r1.dstRIDNext = 7
  r2 := NewRelationshipRecord(storage[2*RelationshipRecordSize():])
  r2.rid = 11
  r2.srcID = 12
  r2.dstID = 13
  r2.srcRIDPrev = 14
  r2.srcRIDNext = 15
  r2.dstRIDPrev = 16
  r2.dstRIDNext = 17
  want := []*RelationshipRecord{r1, nil, r2}
  got := []*RelationshipRecord{GetRelationshipRecord(storage)}
  got = append(got, GetRelationshipRecord(storage[RelationshipRecordSize():]))
  got = append(got, GetRelationshipRecord(storage[2*RelationshipRecordSize():]))
  if !reflect.DeepEqual(got, want) {
    t.Errorf("TestRelationshipRecord got %v; want %v", got, want)
  }
}
