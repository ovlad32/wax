// (c) Copyright 2015 JONNALAGADDA Srinivas
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sparsebitset

// popcount answers the number of bits set to `1` in this word.  It
// uses the bit population count (Hamming Weight) logic taken from
// https://code.google.com/p/go/issues/detail?id=4988#c11.  Original
// by 'https://code.google.com/u/arnehormann/'.
func popcount(x uint64) (n uint64) {
	x -= (x >> 1) & 0x5555555555555555
	x = (x>>2)&0x3333333333333333 + x&0x3333333333333333
	x += x >> 4
	x &= 0x0f0f0f0f0f0f0f0f
	x *= 0x0101010101010101
	return x >> 56
}

// popcountSet answers the number of bits set to `1` in this set.
func popcountSet(a *blockAry) uint64 {
	c := uint64(0)
	for _, aValue := range *a {
		c += popcount(aValue)
	}
	return c
}

// popcountSetAndNot answers the remaining number of bits set to `1`,
// when subtracting another bitset as specified.

func popcountSetAndNot(a, b *blockAry) uint64 {
	c := uint64(0)

	for key, aValue := range *a {
		if bValue, found := (*b)[key]; found {
			c += popcount(aValue &^ bValue)
		} else {
			c += popcount(aValue)
		}
	}
	return c
}

// popcountSetAnd answers the remaining number of bits set to `1`,
// when `and`ed with another bitset.
func popcountSetAnd(a, b *blockAry) uint64 {
	c := uint64(0)
	for key, aValue := range *a {
		if bValue, found := (*b)[key]; found {
			c += popcount(aValue & bValue)
		} else {
			c += popcount(aValue)
		}
	}
	return c
}

// popcountSetOr answers the remaining number of bits set to `1`,
// when inclusively `or`ed with another bitset.
func popcountSetOr(a, b *blockAry) uint64 {
	c := uint64(0)

	for key, aValue := range *a {
		if bValue, found := (*b)[key]; found {
			c += popcount(aValue | bValue)
		} else {
			c += popcount(aValue)
		}
	}
	for key, bValue := range *b {
		if _, found := (*a)[key]; !found {
			c += popcount(bValue)
		}
	}

	return c
}

// popcountSetXor answers the remaining number of bits set to `1`,
// when exclusively `or`ed with another bitset.
func popcountSetXor(a, b *blockAry) uint64 {
	c := uint64(0)

	for key, aValue := range *a {
		if bValue, found := (*b)[key]; found {
			c += popcount(aValue ^ bValue)
		} else {
			c += popcount(aValue)
		}
	}
	for key, bValue := range *b {
		if _, found := (*a)[key]; !found {
			c += popcount(bValue)
		}
	}

	return c

}
