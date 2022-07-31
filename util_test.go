package mydb

import (
	"testing"
)

func Test_appendToSortedRecords(t *testing.T) {
	var init = func() []*record {
		var records []*record
		for i := 1; i < 9; i++ {
			records = append(records, &record{
				Key: toBytes(i),
			})
		}
		return records
	}

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
				l: init(),
				r: &record{Key: toBytes(0)},
			},
		},
		{
			args: args{
				l: init(),
				r: &record{Key: toBytes(11)},
			},
		},
		{
			args: args{
				l: init(),
				r: &record{Key: toBytes(9)},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := appendToSortedRecords(tt.args.l, tt.args.r); !isSorted(got) {
				t.Errorf("appendToSortedRecords() = %v", got)
			}
		})
	}
}
