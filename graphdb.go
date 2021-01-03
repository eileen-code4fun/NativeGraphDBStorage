package graphdb

import (
  "fmt"
  "log"
  "os"
  "syscall"
)

const (
  maxLen = 4096
)

type GraphDB struct {
  inMemory bool
  offsetByNID map[uint16]int
  nodesStorage []byte
  offsetByRID map[uint16]int
  relationshipsStorage []byte
}

func mmap(path string) []byte {
  f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
  if err != nil {
    log.Fatalf("Failed to open node file %s; %v", path, err)
  }
  if err := f.Truncate(maxLen); err != nil {
    log.Fatalf("Failed to resize file %s; %v", path, err)
  }
  data, err := syscall.Mmap(int(f.Fd()), 0, maxLen, syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)
  if err != nil {
    log.Fatalf("Failed to mmap file %s; %v", path, err)
  }
  if err := f.Close(); err != nil {
    log.Fatalf("Failed to close the file %s; %v", path ,err)
  }
  return data
}

func OpenDB(nodesPath, relationshipsPath string) *GraphDB {
  return &GraphDB{
    inMemory: false,
    offsetByNID: map[uint16]int{},
    nodesStorage: mmap(nodesPath),
    offsetByRID: map[uint16]int{},
    relationshipsStorage: mmap(relationshipsPath),
  }
}

func NewInMemoryDB() *GraphDB {
  return &GraphDB{
    inMemory: true,
    offsetByNID: map[uint16]int{},
    nodesStorage: make([]byte, maxLen),
    offsetByRID: map[uint16]int{},
    relationshipsStorage: make([]byte, maxLen),
  }
}

func (g *GraphDB) Close() {
  if g.inMemory {
    return
  }
  if err := syscall.Munmap(g.nodesStorage); err != nil {
    log.Fatalf("Failed to munmap nodes file; %v", err)
  }
  if err := syscall.Munmap(g.relationshipsStorage); err != nil {
    log.Fatalf("Failed to munmap relationships file; %v", err)
  }
}

func (g *GraphDB) AddNode(nid uint16) error {
  if _, has := g.offsetByNID[nid]; has {
    return fmt.Errorf("node %d already exists", nid)
  }
  var i int
  for ; i < maxLen; i += NodeRecordSize() {
    if !InUse(g.nodesStorage[i:]) {
      break
    }
  }
  if i >= maxLen {
    return fmt.Errorf("no more space for node %d", nid)
  }
  n := NewNodeRecord(g.nodesStorage[i:])
  n.nid = nid
  g.offsetByNID[nid] = i
  return nil
}

func (g *GraphDB) getNode(nid uint16) (*NodeRecord, error) {
  offset, ok := g.offsetByNID[nid]
  if !ok {
    return nil, fmt.Errorf("node %d does not exist", nid)
  }
  return GetNodeRecord(g.nodesStorage[offset:]), nil
}

func (g *GraphDB) getRelationship(rid uint16) (*RelationshipRecord, error) {
  offset, ok := g.offsetByRID[rid]
  if !ok {
    return nil, fmt.Errorf("relationship %d does not exist", rid)
  }
  return GetRelationshipRecord(g.relationshipsStorage[offset:]), nil
}

func (g *GraphDB) prependRelationship(n *NodeRecord, r *RelationshipRecord) error {
  first, err := g.getRelationship(n.rid)
  if err != nil {
    return err
  }
  lastRID := first.srcRIDPrev
  if first.dstID == n.nid {
    lastRID = first.dstRIDPrev
  }
  last, err := g.getRelationship(lastRID)
  if err != nil {
    return err
  }
  // Point first.prev to r.
  if first.srcID == n.nid {
    first.srcRIDPrev = r.rid
  } else {
    first.dstRIDPrev = r.rid
  }
  // Point r.next to first.
  // Point r.prev to last.
  if r.srcID == n.nid {
    r.srcRIDNext = first.rid
    r.srcRIDPrev = last.rid
  } else {
    r.dstRIDNext = first.rid
    r.dstRIDPrev = last.rid
  }
  // Point last.next to r.
  if last.srcID == n.nid {
    last.srcRIDNext = r.rid
  } else {
    last.dstRIDNext = r.rid
  }
  return nil
}

func (g *GraphDB) updateRelationships(n *NodeRecord, r *RelationshipRecord) error {
  if n.rid == 0 {
    // First relationship for the node.
    n.rid = r.rid
    return nil
  }
  return g.prependRelationship(n, r)
}

func (g *GraphDB) AddRelationship(srcID, dstID uint16) error {
  if g.RelationshipExists(srcID, dstID) {
    return nil
  }
  // Read nodes from storage.
  srcNode, err := g.getNode(srcID)
  if err != nil {
    return err
  }
  dstNode, err := g.getNode(dstID)
  if err != nil {
    return err
  }
  var i int
  for ; i < maxLen; i += RelationshipRecordSize() {
    if !InUse(g.relationshipsStorage[i:]) {
      break
    }
  }
  if i >= maxLen {
    return fmt.Errorf("no more space for new relationship %d, %d", srcID, dstID)
  }
  newRID := uint16(len(g.offsetByRID)) + 1
  r := NewRelationshipRecord(g.relationshipsStorage[i:])
  r.rid = newRID
  r.srcID = srcID
  r.dstID = dstID
  // All point to itself initially.
  r.srcRIDPrev = newRID
  r.srcRIDNext = newRID
  r.dstRIDPrev = newRID
  r.dstRIDNext = newRID
  g.offsetByRID[r.rid] = i
  if err := g.updateRelationships(srcNode, r); err != nil {
    return err
  }
  if err := g.updateRelationships(dstNode, r); err != nil {
    return err
  }
  return nil
}

// TODO: DeleteNode, DeleteRelationship.

func (g *GraphDB) RelationshipExists(srcID, dstID uint16) bool {
  outs := g.FindOutbounds(srcID)
  for _, dst := range outs {
    if dst == dstID {
      return true
    }
  }
  return false
}

func (g *GraphDB) FindInbounds(nid uint16) []uint16 {
  var ret []uint16
  rs := g.getRelationships(nid)
  for _, r := range rs {
    if r.dstID == nid {
      ret = append(ret, r.srcID)
    }
  }
  return ret
}

func (g *GraphDB) FindOutbounds(nid uint16) []uint16 {
  var ret []uint16
  rs := g.getRelationships(nid)
  for _, r := range rs {
    if r.srcID == nid {
      ret = append(ret, r.dstID)
    }
  }
  return ret
}

func (g *GraphDB) getRelationships(nid uint16) []*RelationshipRecord {
  n, err := g.getNode(nid)
  if err != nil {
    return nil
  }
  var ret []*RelationshipRecord
  next := n.rid
  for {
    r, err := g.getRelationship(next)
    if err != nil {
      return nil
    }
    ret = append(ret, r)
    next = r.srcRIDNext
    if r.dstID == nid {
      next = r.dstRIDNext
    }
    if next == n.rid {
      break
    }
  }
  return ret
}