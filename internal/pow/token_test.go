package pow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	keyA = []byte("key-a-0123456789key-a-0123456789")
	keyB = []byte("key-b-0123456789key-b-0123456789")
)

func testPayload(now time.Time) Payload {
	return Payload{
		ID:           "0197-test-id",
		Sub:          "user-1",
		Iat:          now.Unix(),
		Exp:          now.Add(TokenTTL).Unix(),
		W0:           16384,
		L:            1,
		MinIntervalS: 2,
		MaxBatch:     MaxBatch,
	}
}

// flip corrupts the first base64url character without breaking the encoding.
func flip(tok string) string {
	if tok[0] == 'A' {
		return "B" + tok[1:]
	}
	return "A" + tok[1:]
}

func TestSignParse_RoundTrip(t *testing.T) {
	p := testPayload(time.Now())
	tok, err := Sign(p, keyA)
	require.NoError(t, err)

	got, err := Parse(tok, keyA)
	require.NoError(t, err)
	require.Equal(t, p, got)
}

func TestParse_DualKeyRotation(t *testing.T) {
	tok, err := Sign(testPayload(time.Now()), keyB)
	require.NoError(t, err)
	// current=keyA, previous=keyB: a token signed by the old key still parses
	_, err = Parse(tok, keyA, keyB)
	require.NoError(t, err)
	// but not once the old key leaves the accept window
	_, err = Parse(tok, keyA)
	require.ErrorIs(t, err, ErrBadToken)
}

func TestParse_Tampered(t *testing.T) {
	tok, err := Sign(testPayload(time.Now()), keyA)
	require.NoError(t, err)

	for name, bad := range map[string]string{
		"not base64":   "!!!",
		"too short":    "aGk",
		"flipped byte": flip(tok),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Parse(bad, keyA)
			require.ErrorIs(t, err, ErrBadToken)
		})
	}
}

func TestVerify(t *testing.T) {
	now := time.Now()
	p := testPayload(now)

	require.NoError(t, Verify(p, "user-1", now))
	// exp leeway: 29s past exp is fine, 31s is not (spec §6: 30s leeway)
	exp := time.Unix(p.Exp, 0)
	require.NoError(t, Verify(p, "user-1", exp.Add(29*time.Second)))
	require.ErrorIs(t, Verify(p, "user-1", exp.Add(31*time.Second)), ErrExpired)
	// sub binding kills token farming
	require.ErrorIs(t, Verify(p, "user-2", now), ErrWrongSub)
	empty := p
	empty.Sub = ""
	require.ErrorIs(t, Verify(empty, "", now), ErrWrongSub)
}
