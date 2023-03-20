package set

type Set interface {
	Add(val string) int
	Remove(val string) int
	Has(val string) bool
	Len() int
	ToSlice() []string
	ForEach(consumer func(member string) bool)
	Intersect(another Set) Set
	Union(another Set) Set
	Diff(another Set) Set
	RandomMembers(limit int) []string
	RandomDistinctMembers(limit int) []string
}
