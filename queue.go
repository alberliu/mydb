package mydb

import "container/list"

type Queue[T any] struct {
	list *list.List
}

func NewQueue[T any]() *Queue[T] {
	return &Queue[T]{
		list: &list.List{},
	}
}

func (r *Queue[T]) Push(t *T) {
	r.list.PushBack(t)
}

func (r *Queue[T]) Pop() *T {
	if r.list.Len() == 0 {
		return nil
	}
	front := r.list.Front()
	r.list.Remove(front)
	return front.Value.(*T)
}
