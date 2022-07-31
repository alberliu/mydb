package mydb

import "testing"

func Test_record_match(t *testing.T) {
	type fields struct {
		Key []byte
	}
	type args struct {
		min []byte
		max []byte
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			fields: fields{Key: toBytes(1)},
			args:   args{min: Infinity, max: Infinity},
			want:   true,
		},
		{
			fields: fields{Key: toBytes(1)},
			args:   args{min: toBytes(0), max: toBytes(2)},
			want:   true,
		},
		{
			fields: fields{Key: toBytes(1)},
			args:   args{min: toBytes(2), max: toBytes(3)},
			want:   false,
		},
		{
			fields: fields{Key: toBytes(1)},
			args:   args{min: Infinity, max: toBytes(3)},
			want:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &record{Key: tt.fields.Key}
			if got := r.match(tt.args.min, tt.args.max); got != tt.want {
				t.Errorf("record:%s min:%s min:%s result = %v, want %v",
					string(tt.fields.Key), string(tt.args.min), string(tt.args.max), got, tt.want)
			}
		})
	}
}
