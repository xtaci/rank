package ss

type sortpair struct {
	id    int32
	score int32
}

type SortedSet struct {
	set []sortpair
}

func (ss *SortedSet) Clear() {
	ss.set = nil
}

func (ss *SortedSet) Insert(id, score int32) {
	p := sortpair{id: id, score: score}
	if len(ss.set) == 0 {
		ss.set = []sortpair{p}
		return
	}

	for k := range ss.set {
		if score > ss.set[k].score {
			ss.set = append(ss.set[:k], append([]sortpair{p}, ss.set[k:]...)...)
			return
		}
	}

	// smallest
	ss.set = append(ss.set, p)
}

func (ss *SortedSet) Delete(id int32) {
	for k := range ss.set {
		if ss.set[k].id == id {
			ss.set = append(ss.set[:k], ss.set[k+1:]...)
			return
		}
	}
}

func (ss *SortedSet) Locate(id int32) int32 {
	for k := range ss.set {
		if ss.set[k].id == id {
			return int32(k + 1)
		}
	}
	return -1
}

func (ss *SortedSet) Update(id, score int32) {
	p := sortpair{id: id, score: score}
	old_idx := -1
	insert_idx := len(ss.set)
	for k := range ss.set {
		if old_idx == -1 && ss.set[k].id == id {
			old_idx = k
		} else if insert_idx == len(ss.set) && score > ss.set[k].score { // set once
			insert_idx = k
		}

		if old_idx != -1 && insert_idx != len(ss.set) { // both set, break
			break
		}
	}

	if old_idx == -1 {
		return
	}

	ss.set = append(ss.set[:old_idx], ss.set[old_idx+1:]...)
	if insert_idx > old_idx {
		insert_idx--
	}
	ss.set = append(ss.set[:insert_idx], append([]sortpair{p}, ss.set[insert_idx:]...)...)
}

func (ss *SortedSet) GetList(a, b int) (ids []int32, scores []int32) {
	ids, scores = make([]int32, b-a+1), make([]int32, b-a+1)
	for k := a - 1; k <= b-1; k++ {
		ids[k-a+1] = ss.set[k].id
		scores[k-a+1] = ss.set[k].score
	}
	return
}
