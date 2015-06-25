package ss

import (
	"testing"
)

func TestSS(t *testing.T) {
	const COUNT = 10
	ss := SortedSet{}
	for i := int32(0); i < COUNT; i++ {
		ss.Insert(i, i)
	}
	t.Log(ss)

	t.Log(ss.GetList(0, 9))
	ss.Update(5, 100)
	t.Log(ss)
	ss.Update(0, 200)
	t.Log(ss)
	ss.Update(5, 3)
	t.Log(ss)

	ss.Delete(3)
	t.Log(ss)

	ss.Delete(4)
	t.Log(ss)
	t.Log(ss.GetList(1, 2))
	//	t.Log(ss.GetList(0, 9))
}
