package mapreduce

import "container/heap"

// Struct for word info storage
type WordEntry struct {
	word      string
	count     int
	fileIndex int // Индекс файла, из которого взято слово
}

// min-heap data structure
type WordHeap []WordEntry

// TODO: use optimization - create array size of N right in the constructor, add Clear method
func newWordHeap() *WordHeap {
	minHeap := &WordHeap{}
	heap.Init(minHeap)

	return minHeap
}

func (h WordHeap) Len() int           { return len(h) }
func (h WordHeap) Less(i, j int) bool { return h[i].word < h[j].word }
func (h WordHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *WordHeap) Push(x interface{}) {
	*h = append(*h, x.(WordEntry))
}

func (h *WordHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type Words []string

func (h Words) Len() int           { return len(h) }
func (h Words) Less(i, j int) bool { return h[i] < h[j] }
func (h Words) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *Words) Push(x interface{}) {
	*h = append(*h, x.(string))
}

func (h *Words) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func sortInPlace(strs *[]string) {
	words := Words(*strs)
	heap.Init(&words)
}
