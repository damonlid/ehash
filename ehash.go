package main

import (
	"crypto/sha1"
	"fmt"
	"sort"
	"strconv"
)

//服务器结构体 地址和存储权重
type server struct {
	addr   string
	weight int
}

//当前服务器相关信息
var servers []server

//默认的hash节点数
const defaultNodeNum = 100

//基础虚拟节点
type virtualNode struct {
	nodeKey string
	spotVal uint32
}

type nodes struct {
	virtualNodesArray []virtualNode
}

func (p *nodes) Len() int           { return len(p.virtualNodesArray) }
func (p *nodes) Less(i, j int) bool { return p.virtualNodesArray[i].spotVal < p.virtualNodesArray[j].spotVal }
func (p *nodes) Swap(i, j int)      { p.virtualNodesArray[i], p.virtualNodesArray[j] = p.virtualNodesArray[j], p.virtualNodesArray[i] }
func (p *nodes) Sort()              { sort.Sort(p) }

//生成对应uint32
func getUint32Val(s string) (v uint32) {
	//进行sha1
	h := sha1.New()
	defer h.Reset()
	h.Write([]byte(s))
	hashBytes := h.Sum(nil)
	//go语言的位运算符处理
	if len(hashBytes[4:8]) == 4 {
		v = (uint32(hashBytes[3]) << 24) | (uint32(hashBytes[2]) << 12) | (uint32(hashBytes[1]) << 6) | (uint32(hashBytes[0]) << 3)
	}

	return
}

func (p *nodes) setVirtualNodesArray(servers []server) {
	if len(servers) < 1 {
		return
	}
	//根据权重与节点数，维护一个map - 所有的hash圈上的值对应ip
	for _, v := range servers {
		//第一步计算出每台机器对应的虚拟节点数
		totalVirtualNodeNum := defaultNodeNum * v.weight
		for i := 0; i < totalVirtualNodeNum; i++ {
			iString := strconv.Itoa(i)
			//虚拟节点地址
			virtualAddr := fmt.Sprintf("%s:%s", v.addr, iString)

			virNode := virtualNode{
				nodeKey: v.addr,
				spotVal: getUint32Val(virtualAddr),
			}

			p.virtualNodesArray = append(p.virtualNodesArray, virNode)
		}
		p.Sort()
	}
}

//获取当前数据key对应的存储服务器
func (p *nodes) getNodeSever(w uint32) (addr string){
	i := sort.Search(len(p.virtualNodesArray), func(i int) bool { return p.virtualNodesArray[i].spotVal >= w })
	return p.virtualNodesArray[i].nodeKey
}

func main() {
	vNodes := new(nodes)
	servers = append(servers, server{"127.0.0.1", 1}, server{"127.0.0.2", 2}, server{"127.0.0.3", 3})
	//先赋值，生成虚拟node
	vNodes.setVirtualNodesArray(servers)
	//传入对应的文件名，作为文件key
	fname := "demo.jpg"
	uint32Val := getUint32Val(fname)

	ser := vNodes.getNodeSever(uint32Val)
	fmt.Println("文件对应存储服务器",ser)
}
