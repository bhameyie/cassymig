package migration

// Script represent the set of versioned statement to be applied to the schema
type Script struct {
	Version int
	Stmts   []string
}

//Stack is a Stack taken from Douglas Hall's gist => https://gist.github.com/bemasher/1777766
type Stack struct {
	top  *Element
	size int
}

// Element is an item in the stack
type Element struct {
	value Script // All types satisfy the empty interface, so we can store anything here.
	next  *Element
}

// Len returns the stack's length
func (s *Stack) Len() int {
	return s.size
}

// Push a new element onto the stack
func (s *Stack) Push(value Script) {
	s.top = &Element{value, s.top}
	s.size++
}

// Pop removes the top element from the stack and return it's value
// If the stack is empty, return nil
func (s *Stack) Pop() Script {
	var value Script
	if s.size > 0 {
		value, s.top = s.top.value, s.top.next
		s.size--
		return value
	}
	return value
}
