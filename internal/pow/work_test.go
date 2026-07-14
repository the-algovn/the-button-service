package pow

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func solveFrom(tok string, w0 uint64, l, count uint32, start uint64) uint64 {
	for n := start; ; n++ {
		if CheckWork(tok, w0, l, count, n) {
			return n
		}
	}
}

func TestCheckWork_SolveRoundTrip(t *testing.T) {
	const tok = "opaque-test-challenge"
	nonce := Solve(tok, 64, 2, 8) // expected ~1024 hashes — instant
	require.True(t, CheckWork(tok, 64, 2, 8, nonce))
}

// A solution must not transfer to a different count, token, or nonce. Any
// single perturbed check can pass by luck (p ≈ 1/1024), so scan solutions
// until each binding is demonstrated — expected ~1 iteration each.
func TestCheckWork_BindsInputs(t *testing.T) {
	const tok = "opaque-test-challenge"
	var boundCount, boundToken, boundNonce bool
	nonce := uint64(0)
	for range 50 {
		nonce = solveFrom(tok, 64, 2, 8, nonce)
		if !CheckWork(tok, 64, 2, 9, nonce) {
			boundCount = true
		}
		if !CheckWork("another-token", 64, 2, 8, nonce) {
			boundToken = true
		}
		if !CheckWork(tok, 64, 2, 8, nonce+1) {
			boundNonce = true
		}
		if boundCount && boundToken && boundNonce {
			break
		}
		nonce++
	}
	require.True(t, boundCount, "count is not bound into the hash")
	require.True(t, boundToken, "token is not bound into the hash")
	require.True(t, boundNonce, "nonce is not bound into the hash")
}

func TestCheckWork_DegenerateDifficulty(t *testing.T) {
	// zero factors mean "reject everything" — never divide by zero
	require.False(t, CheckWork("t", 0, 1, 1, 0))
	require.False(t, CheckWork("t", 1, 0, 1, 0))
	require.False(t, CheckWork("t", 1, 1, 0, 0))
	// w0=l=count=1 → target = 2^256: every digest passes
	require.True(t, CheckWork("t", 1, 1, 1, 0))
}
