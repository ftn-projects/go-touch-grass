package skiplist

import (
	"fmt"
	"testing"
)

func equalSkipList(s1 *SkipList, s2 *SkipList) bool {
	if s1.maxHeight != s2.maxHeight || s1.size != s2.size {
		return false
	}

	c1 := s1.head
	c2 := s2.head
	for c1 != nil && c2 != nil {
		if c1.key != c2.key {
			return false
		}
		c1 = c1.next[len(c1.next)-1]
		c2 = c2.next[len(c2.next)-1]
	}
	if c1 != nil || c2 != nil {
		return false
	}
	return true
}

func getSkipList(keys string) *SkipList {
	s := New(10)
	for _, a := range keys {
		k := fmt.Sprintf("%c", a)
		s.Put(k, []byte(k))
	}
	return s
}

func TestFind(t *testing.T) {
	s := getSkipList("abc")
	_, found := s.Get("a")
	if !found {
		t.Errorf("existing not found")
	}

	_, found = s.Get("d")
	if found {
		t.Errorf("found non existing")
	}
}

func TestPut(t *testing.T) {
	got := getSkipList("")

	got.Put("a", []byte("a"))
	want := getSkipList("a")
	if !equalSkipList(got, want) {
		t.Errorf("add")
	}

	got.Put("c", []byte("c"))
	want = getSkipList("ac")
	if !equalSkipList(got, want) {
		t.Errorf("add")
	}

	got.Put("b", []byte("b"))
	want = getSkipList("abc")
	if !equalSkipList(got, want) {
		t.Errorf("add")
	}
}

func TestDelete(t *testing.T) {
	got := getSkipList("abcd")

	got.Delete("a")
	want := getSkipList("bcd")
	if !equalSkipList(got, want) {
		t.Errorf("delete")
	}

	got.Delete("c")
	want = getSkipList("bd")
	if !equalSkipList(got, want) {
		t.Errorf("delete")
	}

	got.Delete("d")
	want = getSkipList("b")
	if !equalSkipList(got, want) {
		t.Errorf("delete")
	}
}

func TestGetAll(t *testing.T) {
	elements := "adeght"
	s := getSkipList(elements)

	data := s.GetAll()
	if len(data) != len(elements) {
		t.Errorf("did not return all values")
	}

	for i, val := range data {
		if val.([]byte)[0] != elements[i] {
			t.Errorf("wrong value")
		}
	}
}
