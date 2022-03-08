package storage

import (
	"context"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"ignis/executor/api/iterator"
	"ignis/executor/core/ierror"
	"ignis/executor/core/iprotocol"
	"ignis/executor/core/itransport"
	"reflect"
)

const IMemoryPartitionType = "IMemoryPartition"

type IMemoryPartition[T any] struct {
	elems  IList
	native bool
}

func NewIMemoryPartition[T any](sz int64, native bool) *IMemoryPartition[T] {
	return &IMemoryPartition[T]{
		NewIList[T](int(sz)),
		native,
	}
}

func ConvIMemoryPartition[T any](other IPartitionBase) IPartitionBase {
	return &IMemoryPartition[T]{
		other.Inner().(IList),
		other.Native(),
	}
}

func (this *IMemoryPartition[T]) Read(transport thrift.TTransport) error {
	zlibTrans, err := itransport.NewIZlibTransport(transport)
	if err != nil {
		return ierror.Raise(err)
	}
	proto := iprotocol.NewIObjectProtocol(zlibTrans)
	elems, err := proto.ReadObject()
	if this.Size() == 0 && reflect.TypeOf((*T)(nil)).Elem().Kind() == reflect.Interface {
		if constructor := registryList[reflect.TypeOf(elems).Elem().String()]; constructor != nil {
			this.elems = constructor((this.elems).Cap())
		}
	}
	this.elems.Merge(elems)
	if err != nil {
		return ierror.Raise(err)
	}
	return nil
}

func (this *IMemoryPartition[T]) Write(transport thrift.TTransport, compression int8) error {
	return this.WriteWithNative(transport, compression, this.native)
}

func (this *IMemoryPartition[T]) WriteWithNative(transport thrift.TTransport, compression int8, native bool) error {
	zlibTrans, err := itransport.NewIZlibTransportWithLevel(transport, int(compression))
	if err != nil {
		return ierror.Raise(err)
	}
	proto := iprotocol.NewIObjectProtocol(zlibTrans)
	err = proto.WriteObjectWithNative(this.elems.Array(), native)
	if err != nil {
		return ierror.Raise(err)
	}
	err = zlibTrans.Flush(context.Background())
	if err != nil {
		return ierror.Raise(err)
	}
	return nil
}

func (this *IMemoryPartition[T]) Clone() (IPartitionBase, error) {
	other := NewIMemoryPartition[T](this.Size(), this.native)
	return other, this.CopyTo(other)
}

func (this *IMemoryPartition[T]) CopyFrom(source IPartitionBase) error {
	if men, ok := source.(*IMemoryPartition[T]); ok {
		this.elems.Merge(men.elems.Array())
	} else {
		other := source.(IPartition[T])
		it, err := other.ReadIterator()
		if err != nil {
			return ierror.Raise(err)
		}
		offset := int(this.Size())
		this.elems.Resize(offset+int(other.Size()), false)
		if elem_impl, ok := this.elems.(*IListImpl[T]); ok {
			for i := 0; i < int(other.Size()); i++ {
				elem, err := it.Next()
				if err != nil {
					return ierror.Raise(err)
				}
				elem_impl.Set(offset, elem)
			}
		} else {
			for i := 0; i < int(other.Size()); i++ {
				elem, err := it.Next()
				if err != nil {
					return ierror.Raise(err)
				}
				elem_impl.SetAny(offset, elem)
			}
		}
	}
	return nil
}

func (this *IMemoryPartition[T]) CopyTo(target IPartitionBase) error {
	return target.CopyFrom(this)
}

func (this *IMemoryPartition[T]) MoveFrom(source IPartitionBase) error {
	if men, ok := source.(*IMemoryPartition[T]); ok {
		if this.Empty() {
			this.elems = men.elems
			men.elems = nil
			return nil
		}
	}
	if err := source.CopyFrom(source); err != nil {
		return ierror.Raise(err)
	}
	return source.Clear()
}

func (this *IMemoryPartition[T]) MoveTo(target IPartitionBase) error {
	return target.MoveFrom(this)
}

func (this *IMemoryPartition[T]) Size() int64 {
	return int64(this.elems.Size())
}

func (this *IMemoryPartition[T]) Empty() bool {
	return this.Size() == 0
}

func (this *IMemoryPartition[T]) Bytes() int64 {
	value := reflect.ValueOf(this.elems.Array())
	sz := this.Size()

	switch value.Elem().Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		return sz * int64(value.Elem().Type().Size())
	case reflect.String:
		return sz * int64(value.Elem().Len())
	default:
		return sz * int64(100)
	}
}

func (this *IMemoryPartition[T]) Clear() error {
	this.elems.Resize(0, false)
	return nil
}

func (this *IMemoryPartition[T]) Fit() error {
	this.elems.Resize(this.elems.Size(), true)
	return nil
}

func (this *IMemoryPartition[T]) Type() string {
	return IMemoryPartitionType
}

func (this *IMemoryPartition[T]) Inner() any {
	return this.elems
}

func (this *IMemoryPartition[T]) Native() bool {
	return this.native
}

func (this *IMemoryPartition[T]) ReadIterator() (iterator.IReadIterator[T], error) {
	if elemsT, ok := this.elems.(*IListImpl[T]); ok {
		return &iMemoryReadIterator[T]{0, int(this.Size()), elemsT}, nil
	}
	return &iMemoryAnyReadIterator[T]{0, int(this.Size()), this.elems}, nil
}

func (this *IMemoryPartition[T]) WriteIterator() (iterator.IWriteIterator[T], error) {
	if elemsT, ok := this.elems.(*IListImpl[T]); ok &&
		(this.Size() > 0 || reflect.TypeOf((*T)(nil)).Elem().Kind() != reflect.Interface) {
		return &iMemoryWriteIterator[T]{elemsT}, nil
	}
	return &iMemoryAnyWriteIterator[T]{false, &this.elems}, nil
}

type iMemoryReadIterator[T any] struct {
	i, sz int
	elems *IListImpl[T]
}

func (this *iMemoryReadIterator[T]) HasNext() bool {
	return this.i < this.sz
}

func (this *iMemoryReadIterator[T]) Next() (t T, err error) {
	t = this.elems.Get(this.i)
	this.i++
	return
}

type iMemoryAnyReadIterator[T any] struct {
	i, sz int
	elems IList
}

func (this *iMemoryAnyReadIterator[T]) HasNext() bool {
	return this.i < this.sz
}

func (this *iMemoryAnyReadIterator[T]) Next() (t T, err error) {
	t = this.elems.GetAny(this.i).(T)
	this.i++
	return
}

type iMemoryWriteIterator[T any] struct {
	elems *IListImpl[T]
}

func (this *iMemoryWriteIterator[T]) Write(v T) error {
	this.elems.Add(v)
	return nil
}

type iMemoryAnyWriteIterator[T any] struct {
	ready bool
	elems *IList
}

func (this *iMemoryAnyWriteIterator[T]) Write(v T) error {
	if !this.ready {
		if (*this.elems).Size() == 0 {
			if constructor := registryList[reflect.TypeOf(v).String()]; constructor != nil {
				(*this.elems) = constructor((*this.elems).Cap())
			}
		}
		this.ready = true
	}
	(*this.elems).AddAny(v)
	return nil
}

//List impl

var registryList = map[string]func(int) IList{}

func CreateList[T any]() {
	registryList[reflect.TypeOf((*T)(nil)).Elem().String()] = func(sz int) IList {
		return NewIList[T](sz)
	}
}

type IList interface {
	Array() any
	Size() int
	Cap() int
	Copy() IList
	Resize(sz int, shrink bool)
	Reserve(sz int)
	GetAny(i int) any
	SetAny(i int, value any)
	AddAny(value any)
	Merge(array any) error
}

func NewIList[T any](sz int) IList {
	return &IListImpl[T]{
		make([]T, sz),
		0,
	}
}

func NewIListArray[T any](array []T) IList {
	return &IListImpl[T]{
		array,
		len(array),
	}
}

type IListImpl[T any] struct {
	array []T
	pos   int
}

func (this *IListImpl[T]) Array() any {
	return this.array[0:this.pos]
}

func (this *IListImpl[T]) Size() int {
	return this.pos
}

func (this *IListImpl[T]) Cap() int {
	return len(this.array)
}

func (this *IListImpl[T]) Copy() IList {
	other := NewIList[T](this.pos)
	copy(other.(*IListImpl[T]).array, this.array)

	return nil
}

func (this *IListImpl[T]) Resize(sz int, shrink bool) {
	if len(this.array) < sz || shrink {
		other := make([]T, sz)
		copy(other, this.array)
		this.array = other
	}
	this.pos = sz
}

func (this *IListImpl[T]) Reserve(sz int) {
	if sz > this.pos {
		other := make([]T, sz)
		copy(other, this.array)
		this.array = other
	}
}

func (this *IListImpl[T]) GetAny(i int) any {
	return this.array[i]
}

func (this *IListImpl[T]) Get(i int) T {
	return this.array[i]
}

func (this *IListImpl[T]) SetAny(i int, value any) {
	this.array[i] = value.(T)
}

func (this *IListImpl[T]) Set(i int, value T) {
	this.array[i] = value
}

func (this *IListImpl[T]) AddAny(value any) {
	if this.pos == len(this.array) {
		this.Reserve(int(float32(len(this.array)) * 1.5))
	}
	this.array[this.pos] = value.(T)
	this.pos++
}

func (this *IListImpl[T]) Merge(array any) error {
	if src, ok := array.([]any); ok {
		this.Reserve(len(this.array) + len(src))
		for i := 0; i < len(src); i++ {
			this.array[this.pos] = src[i].(T)
			this.pos++
		}
	} else if src, ok := array.([]T); ok {
		this.Reserve(len(this.array) + len(src))
		for i := 0; i < len(src); i++ {
			this.array[this.pos] = src[i]
			this.pos++
		}
	} else {
		return ierror.RaiseMsg(fmt.Sprintf("Arrays merge error, src is %s and dest is %s",
			reflect.TypeOf(this.array), reflect.TypeOf(array)))
	}
	return nil
}

func (this *IListImpl[T]) Add(value T) {
	if this.pos == len(this.array) {
		this.Reserve(int(float32(len(this.array)) * 1.5))
	}
	this.array[this.pos] = value
	this.pos++
}
