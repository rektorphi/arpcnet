package util

type Interface interface {
	Len() int
	// Called only once for each index. If true, element at index is moved to the right side of the slice.
	Remove(i int) bool
	Swap(i, j int)
}

func Compact(data Interface) int {
	// compact elements, iterate through array from front and move crap to the back
	rear := data.Len() - 1
	front := 0
	// forward loop, looking for holes
outer:
	for front <= rear {
		if data.Remove(front) {
			// backwards loop, filling holes
			for rear > front {
				if !data.Remove(rear) {
					data.Swap(front, rear)
					rear--
					front++
					continue outer
				}
				rear--
			}
			// reached the front, done
			break outer
		}
		front++
	}
	// now the elements array should have [:front] good elements and rest crap
	return front
}
