package graphdb

import (
  "reflect"
  "testing"
)

func verifyInboundsOutbounds(g *GraphDB, nid uint16, inbounds []uint16, outbounds []uint16, t *testing.T) {
  if got := g.FindInbounds(nid); !reflect.DeepEqual(got, inbounds) {
    t.Errorf("FindInbounds(%d) got %v; want %v", nid, got, inbounds)
  }
  if got := g.FindOutbounds(nid); !reflect.DeepEqual(got, outbounds) {
      t.Errorf("FindOutbounds(%d) got %v; want %v", nid, got, outbounds)
    }
}

func TestGraphDBInMemory(t *testing.T) {
  g := NewInMemoryDB()
  g.AddNode(1)
  g.AddNode(2)
  g.AddNode(3)
  g.AddRelationship(1, 2)
  g.AddRelationship(1, 3)
  g.AddRelationship(2, 3)
  verifyInboundsOutbounds(g, 1, nil, []uint16{2, 3}, t)
  verifyInboundsOutbounds(g, 2, []uint16{1}, []uint16{3}, t)
  verifyInboundsOutbounds(g, 3, []uint16{1, 2}, nil, t)
}