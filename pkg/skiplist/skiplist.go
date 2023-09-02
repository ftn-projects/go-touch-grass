package skiplist

import (
	"fmt"
	"math/rand"
)

type path struct {
	path map[*node][]int
}

func (p *path) add(n *node, i int) {
	if indexes, ok := p.path[n]; ok {
		p.path[n] = append(indexes, i)
	} else {
		p.path[n] = []int{i}
	}
}

func (p *path) adjustAdd(n *node) {
	j := 0
	for node, indexes := range p.path {
		for _, i := range indexes {
			temp := node.next[i]
			node.next[i] = n
			n.next[j] = temp
			j += 1
		}
	}
}

func (p *path) adjustDel(n *node) {
	j := 0
	height := len(n.next)
	for node, indexes := range p.path {
		h := len(node.next)
		for _, i := range indexes {
			if h-i <= height {
				node.next[i] = n.next[j]
				j += 1
			}
		}
	}
}

type SkipList struct {
	maxHeight int
	size      int
	head      *node
}

type node struct {
	key  string
	data interface{}
	next []*node
}

func New(maxHeight int) *SkipList {
	next := make([]*node, maxHeight)

	head := &node{
		"", nil,
		next}

	return &SkipList{maxHeight, 0, head}
}

func (s *SkipList) Get(key string) (interface{}, bool) {
	if s.head == nil {
		return nil, false
	}

	temp := s.head
	for temp != nil {
		if key == temp.key {
			return temp.data, true
		}

		var next *node
		for _, node := range temp.next {
			if node != nil && key >= node.key {
				next = node
				break
			}
		}
		temp = next
	}
	return nil, false
}

func (s *SkipList) Put(key string, data interface{}) {
	height := s.roll()
	p := path{make(map[*node][]int, height)}

	temp := s.head
	var next *node
	for temp != nil {
		if temp.key == key {
			temp.data = data // update if key is found
			return
		}

		next = nil
		h := len(temp.next) // current node height
		for i, node := range temp.next {
			if node == nil || node.key > key {
				if h-i <= height {
					p.add(temp, i)
				}
			} else if node.key <= key {
				next = node
				break
			}
		}
		temp = next
	}

	new := &node{
		key, data,
		make([]*node, height)}
	p.adjustAdd(new)
	s.size++
}

func (s *SkipList) GetAll() []interface{} {
	data := make([]interface{}, s.size)
	current := s.head.next[len(s.head.next)-1]

	i := 0
	for i != s.size {
		data[i] = current.data
		current = current.next[len(current.next)-1]
		i++
	}
	return data
}

func (s *SkipList) Delete(key string) {
	p := path{make(map[*node][]int)}

	temp := s.head
	var next *node
	for temp != nil {
		if temp.key == key {
			break
		}

		next = nil
		for i, node := range temp.next {
			if node != nil && node.key <= key {
				next = node
				if node.key == key {
					p.add(temp, i)
				} else {
					break
				}
			}
		}
		temp = next
	}

	if temp != nil {
		p.adjustDel(temp)
		s.size--
	}
}

func (s *SkipList) Print() {
	current := s.head.next[len(s.head.next)-1]

	for current != nil {
		// fmt.Print(fmt.Sprintf("%c", current.data), ": ")
		fmt.Print(current.data, ": ")
		for i := len(current.next) - 1; i >= 0; i-- {
			next := current.next[i]
			if next != nil {
				fmt.Printf("%s", next.key)
			} else {
				fmt.Print("*")
			}
			if i != 0 {
				fmt.Print("-")
			}
		}
		fmt.Println()

		current = current.next[len(current.next)-1]
	}
}

func (s *SkipList) roll() int {
	level := 1
	// possible ret values from rand are 0 and 1
	// we stop shen we get a 0

	for ; rand.Int31n(2) == 1; level++ {
		if level >= s.maxHeight {
			return level
		}
	}
	return level
}

func (s *SkipList) Size() int {
	return s.size
}

func (s *SkipList) Clear() {
	next := make([]*node, s.maxHeight)
	s.head = &node{"", nil, next}
	s.size = 0
}
