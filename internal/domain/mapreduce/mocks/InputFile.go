// Code generated by mockery v2.43.2. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"

// InputFile is an autogenerated mock type for the InputFile type
type InputFile struct {
	mock.Mock
}

// Close provides a mock function with given fields:
func (_m *InputFile) Close() error {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Close")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Err provides a mock function with given fields:
func (_m *InputFile) Err() error {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Err")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ReadLine provides a mock function with given fields:
func (_m *InputFile) ReadLine() string {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for ReadLine")
	}

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// ReadMappedLine provides a mock function with given fields:
func (_m *InputFile) ReadMappedLine() (string, int, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for ReadMappedLine")
	}

	var r0 string
	var r1 int
	var r2 error
	if rf, ok := ret.Get(0).(func() (string, int, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	if rf, ok := ret.Get(1).(func() int); ok {
		r1 = rf()
	} else {
		r1 = ret.Get(1).(int)
	}

	if rf, ok := ret.Get(2).(func() error); ok {
		r2 = rf()
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// Scan provides a mock function with given fields:
func (_m *InputFile) Scan() bool {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Scan")
	}

	var r0 bool
	if rf, ok := ret.Get(0).(func() bool); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// NewInputFile creates a new instance of InputFile. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewInputFile(t interface {
	mock.TestingT
	Cleanup(func())
}) *InputFile {
	mock := &InputFile{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
