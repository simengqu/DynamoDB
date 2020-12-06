package mydynamo

import "reflect"

type VectorClock struct {
	//todo
	Clock map[string]int
}

//Creates a new VectorClock
func NewVectorClock() VectorClock {
	// panic("todo")
	newClock := VectorClock{
		Clock: map[string]int{},
	}
	return newClock
}

//Returns true if the other VectorClock is causally descended from this one
func (s VectorClock) LessThan(otherClock VectorClock) bool {
	// panic("todo")
	equal := false
	for k, v := range s.Clock {
		if v > otherClock.Clock[k] {
			return false
		} else if v < otherClock.Clock[k] {
			equal = false
		}
	}
	if !equal {
		return true
	}
	return false
}

//Returns true if neither VectorClock is causally descended from the other
func (s VectorClock) Concurrent(otherClock VectorClock) bool {
	// panic("todo")
	if !s.LessThan(otherClock) && !otherClock.LessThan(s) {
		return true
	}
	return false
}

//Increments this VectorClock at the element associated with nodeId
func (s *VectorClock) Increment(nodeId string) {
	// panic("todo")
	s.Clock[nodeId] += 1
}

//Changes this VectorClock to be causally descended from all VectorClocks in clocks
func (s *VectorClock) Combine(clocks []VectorClock) {
	// panic("todo")
	for k, v := range s.Clock {
		for _, c := range clocks {
			if c.Clock[k] > v {
				s.Clock[k] = c.Clock[k]
			}
		}
	}

}

//Tests if two VectorClocks are equal
func (s *VectorClock) Equals(otherClock VectorClock) bool {
	// panic("todo")
	return reflect.DeepEqual(s.Clock, otherClock.Clock)
}
