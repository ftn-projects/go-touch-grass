package cache

import (
	"container/list"
	"fmt"
)

type Cache struct {
	size     int
	list     *list.List
	data_map map[string]*list.Element
}

type Node struct {
	key   string
	value []byte
}

func NewCache(size int) *Cache {
	return &Cache{
		size:     size,
		list:     list.New(),
		data_map: make(map[string]*list.Element),
	}
}

func (c *Cache) Add(key string, value []byte) {
	if element, exists := c.data_map[key]; exists {
		c.list.MoveToFront(element) //ako element postoji, stavi ga na pocetak
		element.Value.(*Node).value = value
	} else {
		newElement := &Node{key: key, value: value}
		if c.list.Len() >= c.size {

			lastElement := c.list.Back()
			if lastElement != nil { //ako je lista puna, obrisi poslednji item
				delete(c.data_map, lastElement.Value.(*Node).key)
				c.list.Remove(lastElement)
			}
		}
		elem := c.list.PushFront(newElement) //dodaj item na pocetak
		c.data_map[key] = elem
	}
}

func (c *Cache) Get(key string) ([]byte, bool) {
	if element, exists := c.data_map[key]; exists { //bool je da li element postoji, drugo je sam element
		c.list.MoveToFront(element)
		return element.Value.(*Node).value, true
	}

	return nil, false
}

func (c *Cache) Remove(key string) bool { //vraca true ako je bio u listi, false ako nije
	if element, exists := c.data_map[key]; exists {
		delete(c.data_map, key)
		c.list.Remove(element)
		return true
	}
	return false
}

func (c *Cache) PrintCache() {

	fmt.Println("Cache:")
	for element := c.list.Front(); element != nil; element = element.Next() {
		node := element.Value.(*Node)
		fmt.Printf("Key: %s, Value: %s\n", node.key, string(node.value))
	}
}
