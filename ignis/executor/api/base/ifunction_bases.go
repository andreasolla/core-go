package base

import (
	"ignis/executor/api/function"
	"ignis/executor/api/ipair"
	"ignis/executor/api/iterator"
	"ignis/executor/core/modules/impl"
)

type IExecuteToAbs interface {
	RunExecuteTo(i *impl.IPipeImpl, f function.IBaseFunction) error
}

type IExecuteTo[R any] struct {
}

func (this *IExecuteTo[R]) RunExecuteTo(i *impl.IPipeImpl, f function.IBaseFunction) error {
	RegisterType[R](i.Context())
	return impl.ExecuteTo[R](i, f.(function.IFunction0[[][]R]))
}

type IMapAbs interface {
	RunMap(i *impl.IPipeImpl, f function.IBaseFunction) error
}

type IMap[T any, R any] struct {
}

func (this *IMap[T, R]) RunMap(i *impl.IPipeImpl, f function.IBaseFunction) error {
	RegisterType[T](i.Context())
	RegisterType[R](i.Context())
	return impl.Map[T, R](i, f.(function.IFunction[T, R]))
}

type IFilterAbs interface {
	RunFilter(i *impl.IPipeImpl, f function.IBaseFunction) error
}

type IFilter[T any] struct {
}

func (this *IFilter[T]) RunFilter(i *impl.IPipeImpl, f function.IBaseFunction) error {
	RegisterType[T](i.Context())
	return impl.Filter[T](i, f.(function.IFunction[T, bool]))
}

type IFlatmapAbs interface {
	RunFlatmap(i *impl.IPipeImpl, f function.IBaseFunction) error
}

type IFlatmap[T any, R any] struct {
}

func (this *IFlatmap[T, R]) RunFlatmap(i *impl.IPipeImpl, f function.IBaseFunction) error {
	RegisterType[T](i.Context())
	RegisterType[R](i.Context())
	return impl.Flatmap[T, R](i, f.(function.IFunction[T, []R]))
}

type IKeyByAbs interface {
	RunKeyBy(i *impl.IPipeImpl, f function.IBaseFunction) error
}

type IKeyBy[T any, R comparable] struct {
}

func (this *IKeyBy[T, R]) RunKeyBy(i *impl.IPipeImpl, f function.IBaseFunction) error {
	RegisterTypeWithKey[R, T](i.Context())
	return impl.KeyBy[T, R](i, f.(function.IFunction[T, R]))
}

type IMapPartitionsAbs interface {
	RunMapPartitions(i *impl.IPipeImpl, f function.IBaseFunction) error
}

type IMapPartitions[T any, R any] struct {
}

func (this *IMapPartitions[T, R]) RunMapPartitions(i *impl.IPipeImpl, f function.IBaseFunction) error {
	RegisterType[T](i.Context())
	RegisterType[R](i.Context())
	return impl.MapPartitions[T, R](i, f.(function.IFunction[iterator.IReadIterator[T], []R]))
}

type IMapPartitionsWithIndexAbs interface {
	RunMapPartitionsWithIndex(i *impl.IPipeImpl, f function.IBaseFunction, preservesPartitioning bool) error
}

type IMapPartitionsWithIndex[T any, R any] struct {
}

func (this *IMapPartitions[T, R]) RunMapPartitionsWithIndex(i *impl.IPipeImpl, f function.IBaseFunction, preservesPartitioning bool) error {
	RegisterType[T](i.Context())
	RegisterType[R](i.Context())
	return impl.MapPartitionsWithIndex[T, R](i, f.(function.IFunction2[int64, iterator.IReadIterator[T], []R]), preservesPartitioning)
}

type IMapExecutorAbs interface {
	RunMapExecutor(i *impl.IPipeImpl, f function.IBaseFunction) error
}

type IMapExecutor[T any] struct {
}

func (this *IMapExecutor[T]) RunMapExecutor(i *impl.IPipeImpl, f function.IBaseFunction) error {
	RegisterType[T](i.Context())
	return impl.MapExecutor[T](i, f.(function.IVoidFunction[[][]T]))
}

type IMapExecutorToAbs interface {
	RunMapExecutorTo(i *impl.IPipeImpl, f function.IBaseFunction) error
}

type IMapExecutorTo[T any, R any] struct {
}

func (this *IMapExecutorTo[T, R]) RunMapExecutorTo(i *impl.IPipeImpl, f function.IBaseFunction) error {
	RegisterType[T](i.Context())
	RegisterType[R](i.Context())
	return impl.MapExecutorTo[T, R](i, f.(function.IFunction[[][]T, [][]R]))
}

type ISortByAbs interface {
	RunSortBy(i *impl.ISortImpl, f function.IBaseFunction, ascending bool) error
	RunSortByWithPartitions(i *impl.ISortImpl, f function.IBaseFunction, ascending bool, partitions int64) error
}

type ISortBy[T any] struct {
}

func (this *ISortBy[T]) RunSortBy(i *impl.ISortImpl, f function.IBaseFunction, ascending bool) error {
	RegisterType[T](i.Context())
	return impl.SortBy(i, f.(function.IFunction2[T, T, bool]), ascending)
}

func (this *ISortBy[T]) RunSortByWithPartitions(i *impl.ISortImpl, f function.IBaseFunction, ascending bool, partitions int64) error {
	RegisterType[T](i.Context())
	return impl.SortByWithPartitions(i, f.(function.IFunction2[T, T, bool]), ascending, partitions)
}

type ITopByAbs interface {
	RunTopBy(i *impl.ISortImpl, f function.IBaseFunction, n int64) error
}

type ITopBy[T any] struct {
}

func (this *ITopBy[T]) RunTopBy(i *impl.ISortImpl, f function.IBaseFunction, n int64) error {
	RegisterType[T](i.Context())
	return impl.TopBy(i, f.(function.IFunction2[T, T, bool]), n)
}

type ITakeOrderedByAbs interface {
	RunTakeOrderedBy(i *impl.ISortImpl, f function.IBaseFunction, n int64) error
}

type ITakeOrderedBy[T any] struct {
}

func (this *ITakeOrderedBy[T]) RunTakeOrderedBy(i *impl.ISortImpl, f function.IBaseFunction, n int64) error {
	RegisterType[T](i.Context())
	return impl.TakeOrderedBy(i, f.(function.IFunction2[T, T, bool]), n)
}

type IMaxByAbs interface {
	RunMaxBy(i *impl.ISortImpl, f function.IBaseFunction) error
}

type IMaxBy[T any] struct {
}

func (this *IMaxBy[T]) RunMaxBy(i *impl.ISortImpl, f function.IBaseFunction) error {
	RegisterType[T](i.Context())
	return impl.MaxBy(i, f.(function.IFunction2[T, T, bool]))
}

type IMinByAbs interface {
	RunMinBy(i *impl.ISortImpl, f function.IBaseFunction) error
}

type IMinBy[T any] struct {
}

func (this *IMinBy[T]) RunMinBy(i *impl.ISortImpl, f function.IBaseFunction) error {
	RegisterType[T](i.Context())
	return impl.MinBy(i, f.(function.IFunction2[T, T, bool]))
}

type ISortByKeyAbs interface {
	RunSortByKey(i *impl.ISortImpl, f function.IBaseFunction, ascending bool) error
	RunSortByKeyWithPartitions(i *impl.ISortImpl, f function.IBaseFunction, ascending bool, partitions int64) error
}

type ISortByKey[K any, V any] struct {
}

func (this *ISortByKey[K, V]) RunSortByKey(i *impl.ISortImpl, f function.IBaseFunction, ascending bool) error {
	RegisterType[K](i.Context())
	RegisterType[V](i.Context())
	RegisterType[ipair.IPair[K, V]](i.Context())
	return impl.SortByKeyBy[V, K](i, f.(function.IFunction2[K, K, bool]), ascending)
}

func (this *ISortByKey[K, V]) RunSortByKeyWithPartitions(i *impl.ISortImpl, f function.IBaseFunction, ascending bool, partitions int64) error {
	RegisterType[K](i.Context())
	RegisterType[V](i.Context())
	RegisterType[ipair.IPair[K, V]](i.Context())
	return impl.SortByKeyByWithPartitions[V, K](i, f.(function.IFunction2[K, K, bool]), ascending, partitions)
}
