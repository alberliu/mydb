package mydb

const defaultPageSize = 4096

// options 初始化参数
type options struct {
	pageSize uint64
}

type Option interface {
	apply(*options)
}

type funcOption struct {
	f func(*options)
}

func (fdo *funcOption) apply(do *options) {
	fdo.f(do)
}

func newFuncServerOption(f func(*options)) *funcOption {
	return &funcOption{
		f: f,
	}
}

// WithPageSize 设置页大小,默认值是4K
func WithPageSize(pageSize uint64) Option {
	if pageSize < defaultPageSize || pageSize%defaultPageSize != 0 {
		panic("pageSize must greater or equal to 4096 and remainder of 4096 is zero")
	}

	return newFuncServerOption(func(o *options) {
		o.pageSize = pageSize
	})
}

func getOptions(opts ...Option) *options {
	options := &options{
		pageSize: 4096,
	}

	for _, o := range opts {
		o.apply(options)
	}

	return options
}
