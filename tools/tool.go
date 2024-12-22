package tools

import (
	"crypto/sha1"
	"fmt"
	"math/big"
)

var IdentifierLength = 10 // m, the length of the identifier, default is 10

// you should set the identifier length before using the tools
// if not, the default value will be used
func SetIdentifierLength(length int) {
	IdentifierLength = length
}

// 2^m
var TwoM = new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(IdentifierLength)), nil)

// x % 2^m == x & (2^m - 1), wiki: https://en.wikipedia.org/wiki/Modulo
var TwoMMinusOne = new(big.Int).Sub(TwoM, big.NewInt(1))

// infinity: 2^m + 1
var Infinity = new(big.Int).Add(TwoM, big.NewInt(1))

// convert string to *big.Int
func HexStringToBigInt(str string) (*big.Int, error) {
	if bigInt, success := new(big.Int).SetString(str, 16); success {
		return bigInt, nil
	}
	return nil, fmt.Errorf("failed to convert string to *big.Int")
}

// generate hash
func GenerateHash(elt string) *big.Int {
	hashes := sha1.New() // use sha1 now, but can be changed to other hash functions
	hashes.Write([]byte(elt))
	return new(big.Int).SetBytes(hashes.Sum(nil))
}

// generate identifier, normal situation
func GenerateIdentifier(name string) *big.Int {
	// generate the hash of the name
	temp := GenerateHash(name)
	// return the hash mod 2^m
	return temp.And(temp, TwoMMinusOne)
}

// LessThan returns true if a < b
func LessThan(a, b *big.Int) bool {
	return a.Cmp(b) < 0
}

// LessThanOrEqual returns true if a <= b
func LessThanOrEqual(a, b *big.Int) bool {
	return a.Cmp(b) <= 0
}

// GreaterThan returns true if a > b
func GreaterThan(a, b *big.Int) bool {
	return a.Cmp(b) > 0
}

// greaterThan returns true if a > b
func GreaterThanOrEqual(a, b *big.Int) bool {
	return a.Cmp(b) >= 0
}

// InInterval returns true if x is in the interval (a, b) or [a, b] or (a, b] or [a, b).
func InInterval(x, a, b *big.Int, leftClosed, rightClosed bool) bool {
	if leftClosed && rightClosed {
		return GreaterThanOrEqual(x, a) && LessThanOrEqual(x, b)
	} else if leftClosed {
		return GreaterThanOrEqual(x, a) && LessThan(x, b)
	} else if rightClosed {
		return GreaterThan(x, a) && LessThanOrEqual(x, b)
	} else {
		return GreaterThan(x, a) && LessThan(x, b)
	}
}

// ModIntervalCheck returns true if x is in the modular interval (a, b) or [a, b] or (a, b] or [a, b).
// example 1: (22, 22) means from 22 to mod and 0 to 22,
// example 2: (22, 12) means from 22 to mod and 0 to 12,
// example 3: (12, 22) means from 12 to 22.
func ModIntervalCheck(x, a, b *big.Int, leftClosed, rightClosed bool) bool {
	mod := TwoM
	if a.Cmp(b) < 0 {
		// a < b, normal interval, eg. (a, b)
		return InInterval(x, a, b, leftClosed, rightClosed)
	} else {
		// a >= b, mod interval, eg. (a, mod) or [0, b)
		return InInterval(x, a, mod, leftClosed, false) || InInterval(x, big.NewInt(0), b, true, rightClosed)
	}
}
