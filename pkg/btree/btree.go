package btree

import (
	"fmt"
)

type BTree struct {
	rang int
	size int
	root *node
}

type item struct {
	key  string
	data interface{}
}

type node struct {
	is_leaf  bool
	count    int
	rang     int
	items    []item
	children []*node
}

func (n *node) putItem(newItem item) (addition bool) {
	i := n.count - 1 // we start from the rightest element in node (if we need to move keys)
	if !n.is_leaf {
		// find a child which is going to have the new key
		for i >= 0 && n.items[i].key > newItem.key {
			i--
		}
		// checking if child is full
		if n.children[i+1].count == 2*n.rang-1 {
			// if it is split the child
			n.splitChild(i+1, n.children[i+1])
			// checking which child is going to have a key after a split
			if n.items[i+1].key < newItem.key {
				i++
			}
		}
		addition = n.children[i+1].putItem(newItem)
		return
	}

	i = n.getKeyIndex(newItem.key)
	if i == -1 {
		i = n.count - 1
		// find place for a new item and move greater keys
		for i >= 0 && n.items[i].key > newItem.key {
			n.items[i+1] = n.items[i]
			i--
		}
		// place a new item
		n.items[i+1] = newItem
		n.count++
		addition = true
	} else {
		n.items[i] = newItem // updating if existing
	}
	return
}

func (n *node) getKeyIndex(key string) int {
	for i, item := range n.items {
		if item.key == key {
			return i
		}
	}
	return -1
}

func (n *node) splitChild(i int, child *node) {

	// New node that is going to store t-1 keys of a child
	newNode := newNode(child.is_leaf, n.rang)
	newNode.count = n.rang - 1

	// Copy keys
	for j := 0; j < n.rang-1; j++ {
		newNode.items[j] = child.items[j+child.rang]
	}

	// if node was not leaf copy it's children
	if !child.is_leaf {
		for j := 0; j < n.rang; j++ {
			newNode.children[j] = child.children[j+n.rang]
		}
	}
	// change number of keys in child
	child.count = n.rang - 1

	// creating space for new child
	for j := n.count; j >= i+1; j-- {
		n.children[j+1] = n.children[j]
	}
	// attacthing new child
	n.children[i+1] = newNode

	// placeing a new key
	for j := n.count - 1; j >= i; j-- {
		n.items[j+1] = n.items[j]
	}
	n.items[i] = child.items[n.rang-1]

	// changing number of keys in node
	n.count++
}

func New(degree int) *BTree {
	return &BTree{
		rang: degree,
	}
}

func newNode(is_leaf bool, degree int) *node {
	return &node{
		is_leaf:  is_leaf,
		rang:     degree,
		children: make([]*node, 2*degree),
		items:    make([]item, 2*degree-1),
	}
}

func (bt *BTree) Put(key string, data interface{}) {
	// Function for inserting new key or updating current data of that key
	var added bool
	newItem := item{key, data}

	if bt.root == nil { // Checking if Tree is empty, if it is add root
		bt.root = newNode(true, bt.rang)
		bt.root.items[0] = newItem
		bt.root.count++
		bt.size++
		return
	}

	if bt.root.count == 2*bt.root.rang-1 { // checking if root is full
		// creating a new root
		newRoot := newNode(false, bt.rang)

		// making old root a child of a new root
		newRoot.children[0] = bt.root

		// split old root and move 1 key to the new root
		newRoot.splitChild(0, bt.root)

		// chosing which child is going to have a new key
		i := 0
		if newRoot.items[0].key < key {
			i++
		}
		added = newRoot.children[i].putItem(newItem)
		bt.root = newRoot

	} else { // if it's not full then add a new key
		added = bt.root.putItem(newItem)
	}

	if added {
		bt.size++
	}
}

func (n *node) traverse() {
	var i int // We use it after for to traverse last child

	//it's on position n.count
	for i = 0; i < n.count; i++ {
		if !n.is_leaf {
			n.children[i].traverse()
		}
		fmt.Print(" ", n.items[i])
	}
	if !n.is_leaf {
		n.children[i].traverse()
	}
}

func (bt *BTree) Print() {
	// Function used to traverse through the BTree
	// Same algorithm used for getting data sorted from tree
	bt.root.traverse()
}

func (bt *BTree) Delete(key string) {
	if bt.root == nil {
		return
	}

	bt.root.delete(key)

	// if root has 0 keys and is leaf then set it to NIL
	if bt.root.count == 0 {

		if bt.root.is_leaf {
			bt.root = nil
		}
		bt.root = bt.root.children[0]
	}
}

func (n *node) delete(key string) {
	i := n.findKey(key)
	// The key is in this node
	if i < n.count && n.items[i].key == key {
		if n.is_leaf {
			n.removeFromLeaf(i)
			return
		}
		n.removeFromInternalNode(i)
	} else {

		if n.is_leaf {
			fmt.Println("Key not founded")
			return
		}
		flag := i == n.count
		if n.children[i].count < n.rang {
			n.fill(i)
		}

		if flag && i > n.count {
			n.children[i-1].delete(key)
		} else {
			n.children[i].delete(key)
		}
	}

}

func (n *node) findKey(key string) int {
	// find the key or the node that contains the key
	i := 0
	for i < n.count && n.items[i].key < key {
		i++
	}
	return i
}

func (n *node) removeFromInternalNode(index int) {

	if n.children[index].count >= n.rang {
		pred := n.getPredcessor(index)
		n.items[index] = pred
		n.children[index].delete(pred.key)
	} else if n.children[index+1].count >= n.rang {
		succ := n.getSuccessor(index)
		n.items[index] = succ
		n.children[index+1].delete(succ.key)
	} else {
		n.merge(index)
		k := n.items[index].key
		n.children[index].delete(k)
	}

}

func (n *node) removeFromLeaf(index int) {
	for i := index + 1; i < n.count; i++ {
		n.items[i-1] = n.items[i]
	}
	n.count--
}

func (n *node) merge(index int) {
	// Merging child on index and index + 1
	var child *node = n.children[index]
	var sibling *node = n.children[index+1]
	var degree int = n.rang
	child.items[n.rang-1] = n.items[index]

	for i := 0; i < n.children[i+1].count; i++ {
		child.items[i+degree] = sibling.items[i]
	}

	if !child.is_leaf {
		for i := 0; i <= sibling.count; i++ {
			child.children[i+degree] = sibling.children[i]
		}
	}

	for i := index + 1; i < n.count; i++ {
		n.items[i-1] = n.items[i]
	}

	for i := index + 2; i <= n.count; i++ {
		n.children[i-1] = n.children[i]
	}

	child.count += sibling.count + 1
	n.count -= 1

}

func (n *node) fill(index int) {
	if index != 0 && n.children[index-1].count >= n.rang {
		// if child before index-th has more than t-1 keys
		n.borrowFromPreviousChild(index)

	} else if index != n.count && n.children[index+1].count >= n.rang {
		// if child after index-th has more than t-1 keys
		n.borrowFromNextChild(index)
	} else {
		// merge sibling
		if index != n.count {
			n.merge(index)
		} else {
			n.merge(index - 1)
		}
	}
}

func (n *node) borrowFromNextChild(index int) {
	// Borrows a key from a child on index + 1
	var child *node = n.children[index]
	var rightSibling *node = n.children[index+1]

	// movid a key ond index to child
	child.items[child.count] = n.items[index]
	if !child.is_leaf {
		child.children[child.count+1] = rightSibling.children[0]
	}

	n.items[index] = rightSibling.items[0]

	for i := 1; i < rightSibling.count; i++ {
		rightSibling.items[i-1] = rightSibling.items[i]
	}

	if !rightSibling.is_leaf {
		for i := 1; i <= rightSibling.count; i++ {
			rightSibling.children[i-1] = rightSibling.children[i]
		}
	}
	child.count++
	rightSibling.count--
}

func (n *node) borrowFromPreviousChild(index int) {
	// Borrows a key from a child on index - 1
	var child *node = n.children[index]
	var leftSibling *node = n.children[index-1]

	for i := child.count - 1; i >= 0; i-- {
		child.items[i+1] = child.items[i]
	}

	if !child.is_leaf {
		for i := child.count; i >= 0; i++ {
			child.children[i+1] = child.children[i]
		}
	}

	child.items[0] = n.items[index-1]
	if !child.is_leaf {
		child.children[0] = leftSibling.children[leftSibling.count]
	}

	n.items[index-1] = leftSibling.items[leftSibling.count-1]

	child.count++
	leftSibling.count--
}

func (n *node) getSuccessor(index int) item {
	var current *node = n.children[index+1]
	for !current.is_leaf {
		current = current.children[current.count]
	}
	return current.items[current.count-1]
}

func (n *node) getPredcessor(index int) item {
	var current *node = n.children[index]
	for !current.is_leaf {
		current = current.children[0]
	}
	return current.items[0]
}

func (bt *BTree) Find(key string) (interface{}, bool) {
	return bt.root.search(key)
}

func (n *node) search(key string) (interface{}, bool) {
	i := 0
	for i < n.count && key > n.items[i].key {
		i++
	}

	if n.count > i && n.items[i].key == key {
		return n.items[i].data, true
	}

	if n.is_leaf {
		return nil, false
	}
	return n.children[i].search(key)
}

func (n *node) getAll(data []interface{}, j *int) {
	i := 0
	for i = 0; i < n.count; i++ {
		if !n.is_leaf {
			n.children[i].getAll(data, j)
		}

		data[*j] = n.items[i].data
		*j++
	}
	if !n.is_leaf {
		n.children[i].getAll(data, j)
	}
}

func (bt *BTree) GetAll() []interface{} {
	j := 0
	data := make([]interface{}, bt.size)
	bt.root.getAll(data, &j)
	return data
}
