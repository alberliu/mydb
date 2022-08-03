package mydb

import (
	"bytes"
	"testing"
)

func initRecordList() []*record {
	var records []*record
	for i := 1; i < 9; i++ {
		records = append(records, &record{
			Key: toBytes(i),
		})
	}
	return records
}

func Test_appendToSortedRecords(t *testing.T) {
	type args struct {
		l []*record
		r *record
	}
	tests := []struct {
		name string
		args args
	}{
		{
			args: args{
				l: initRecordList(),
				r: &record{Key: toBytes(0)},
			},
		},
		{
			args: args{
				l: initRecordList(),
				r: &record{Key: toBytes(11)},
			},
		},
		{
			args: args{
				l: initRecordList(),
				r: &record{Key: toBytes(9)},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, _ := appendToSortedRecords(tt.args.l, tt.args.r); !isSorted(got) {
				t.Errorf("appendToSortedRecords() = %v", got)
			}
		})
	}
}

func Test_newRecordList(t *testing.T) {
	list := newRecordList(initRecordList())
	list.append(&record{Key: toBytes(0)})
	r := list.get(toBytes(0))
	if r == nil {
		t.Fatal()
	}

	list.update(&record{Key: toBytes(0), Value: toBytes(1)})
	r = list.get(toBytes(0))
	if r == nil || !bytes.Equal(r.Value, toBytes(1)) {
		t.Fatal()
	}

	key := toBytes(1)
	list.delete(key)
	r = list.get(key)
	if r != nil {
		t.Fatal()
	}
}
