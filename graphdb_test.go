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

func testGraph(g *GraphDB, t *testing.T) {
  if err := g.AddNode(1); err != nil {
    t.Errorf("failed to add node %d; %v", 1, err)
  }
  if err := g.AddNode(2); err != nil {
    t.Errorf("failed to add node %d; %v", 2, err)
  }
  if err := g.AddNode(3); err != nil {
    t.Errorf("failed to add node %d; %v", 3, err)
  }
  if err := g.AddNode(4); err != nil {
    t.Errorf("failed to add node %d; %v", 4, err)
  }
  if err := g.AddNode(5); err != nil {
    t.Errorf("failed to add node %d; %v", 5, err)
  }
  if err := g.AddNode(6); err != nil {
    t.Errorf("failed to add node %d; %v", 6, err)
  }
  if err := g.AddRelationship(1, 2); err != nil {
    t.Errorf("failed to add relationship %d->%d; %v", 1, 2, err)
  }
  if err := g.AddRelationship(1, 3); err != nil {
    t.Errorf("failed to add relationship %d->%d; %v", 1, 3, err)
  }
  if err := g.AddRelationship(2, 3); err != nil {
    t.Errorf("failed to add relationship %d->%d; %v", 2, 3, err)
  }
  if err := g.AddRelationship(2, 4); err != nil {
    t.Errorf("failed to add relationship %d->%d; %v", 2, 4, err)
  }
  if err := g.AddRelationship(3, 5); err != nil {
    t.Errorf("failed to add relationship %d->%d; %v", 3, 5, err)
  }
  if err := g.AddRelationship(3, 6); err != nil {
    t.Errorf("failed to add relationship %d->%d; %v", 3, 6, err)
  }
  verifyInboundsOutbounds(g, 1, nil, []uint16{2, 3}, t)
  verifyInboundsOutbounds(g, 2, []uint16{1}, []uint16{3, 4}, t)
  verifyInboundsOutbounds(g, 3, []uint16{1, 2}, []uint16{5, 6}, t)
  verifyInboundsOutbounds(g, 4, []uint16{2}, nil, t)
  verifyInboundsOutbounds(g, 5, []uint16{3}, nil, t)
  verifyInboundsOutbounds(g, 6, []uint16{3}, nil, t)
}

func TestGraphDBInMemory(t *testing.T) {
  g := NewInMemoryDB()
  defer g.Close()
  testGraph(g, t)
}

func TestGraphDBInFile(t *testing.T) {
  g := OpenDB("nodes.db", "relationships.db")
  defer g.Close()
  testGraph(g, t)
}