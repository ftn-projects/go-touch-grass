package merkle

import (
	"crypto/sha1"
	"encoding/hex"
	"os"
	"strings"
)

type MerkleRoot struct {
	root *Node
}

func (mr *MerkleRoot) String() string {
	return mr.root.String()
}

type Node struct {
	data  []byte
	left  *Node
	right *Node
}

func (n *Node) String() string {
	return hex.EncodeToString(n.data[:])
}
func ToHex(data string) []byte {
	v, _ := hex.DecodeString(data)
	return v
}

func Hash(data []byte) [20]byte {
	return sha1.Sum(data)
}
func newEmptyNode() *Node {
	return &Node{data: make([]byte, 20)}
}
func newNode(hash []byte, leftc *Node, rightc *Node) *Node {
	return &Node{data: hash, left: leftc, right: rightc}
}
func NewMerkleTree(leafs []*Node) *MerkleRoot {

	root := &MerkleRoot{root: getMerkleRoot(leafs)}
	return root
}
func getMerkleRoot(nodes []*Node) *Node {
	if len(nodes) == 1 {
		return nodes[0]
	}

	if len(nodes)%2 != 0 {
		nodes = append(nodes, newEmptyNode())
	}
	newLevel := make([]*Node, 0)

	for i := 0; i < len(nodes); i += 2 {
		value := append(nodes[i].data, nodes[i+1].data...)
		hash := Hash(value)
		newLevel = append(newLevel, newNode(hash[:], nodes[i], nodes[i+1]))
	}

	return getMerkleRoot(newLevel)
}

func GetLeaf(data []byte) *Node {
	hash := Hash(data)
	return &Node{data: hash[:], left: nil, right: nil}
}

func (m *MerkleRoot) Save(filepath string) {
	var result []string
	serializeMerkle(m.root, &result)
	err := writeMerkle(filepath, strings.Join(result, "\n"))
	if err != nil {
		panic(err)
	}
}

func TryLoad(filepath string) *MerkleRoot {
	data, err := os.ReadFile(filepath)
	if err != nil {
		panic(err)
	}
	m := &MerkleRoot{}
	m.root = readMerkle(string(data))
	return m
}

func serializeMerkle(node *Node, result *[]string) {
	if node == nil {
		*result = append(*result, "nil")
		return
	}
	*result = append(*result, node.String())
	serializeMerkle(node.left, result)
	serializeMerkle(node.right, result)

}
func readMerkle(data string) *Node {
	nodes := strings.Split(data, "\n")
	index := 0
	return deserializeMerkle(nodes, &index)
}
func deserializeMerkle(nodes []string, index *int) *Node {
	if *index >= len(nodes) || nodes[*index] == "nil" {
		*index++
		return nil
	}

	newNode := &Node{data: ToHex(nodes[*index])}
	*index++
	newNode.left = deserializeMerkle(nodes, index)
	newNode.right = deserializeMerkle(nodes, index)
	return newNode
}

func writeMerkle(filepath string, result string) error {
	return os.WriteFile(filepath, []byte(result), 0644)
}
