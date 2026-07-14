package pow

import (
	"crypto/sha256"
	"encoding/binary"
	"math/big"
)

// two256 is 2^256, the numerator of the smooth full-target form (spec §5).
var two256 = new(big.Int).Lsh(big.NewInt(1), 256)

// target returns 2^256 / (w0 * count * l); nil means "reject everything".
func target(w0 uint64, l, count uint32) *big.Int {
	d := new(big.Int).SetUint64(w0)
	d.Mul(d, new(big.Int).SetUint64(uint64(count)))
	d.Mul(d, new(big.Int).SetUint64(uint64(l)))
	if d.Sign() == 0 {
		return nil
	}
	return new(big.Int).Div(two256, d)
}

// CheckWork verifies spec §5: SHA-256(tokenBytes || be32(count) || be64(nonce)),
// read as a big-endian 256-bit integer, must be < 2^256/(w0*count*l).
// tokenBytes are the ASCII bytes of the challenge string exactly as issued —
// the SPA solver hashes the same bytes.
func CheckWork(token string, w0 uint64, l, count uint32, nonce uint64) bool {
	tgt := target(w0, l, count)
	if tgt == nil {
		return false
	}
	h := sha256.New()
	h.Write([]byte(token))
	var suffix [12]byte
	binary.BigEndian.PutUint32(suffix[0:4], count)
	binary.BigEndian.PutUint64(suffix[4:12], nonce)
	h.Write(suffix[:])
	return new(big.Int).SetBytes(h.Sum(nil)).Cmp(tgt) < 0
}

// Solve brute-forces the smallest nonce satisfying CheckWork. Test helper —
// production clients solve in a WASM Web Worker.
func Solve(token string, w0 uint64, l, count uint32) uint64 {
	for nonce := uint64(0); ; nonce++ {
		if CheckWork(token, w0, l, count, nonce) {
			return nonce
		}
	}
}
