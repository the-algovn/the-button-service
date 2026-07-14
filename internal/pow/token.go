// Package pow implements the stateless proof-of-work protocol (spec §5):
// HMAC-signed challenge tokens, the SHA-256 work check, and the shared
// difficulty controller's pure math.
package pow

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"
)

const (
	// MaxBatch is the hard batch-size cap (spec §5).
	MaxBatch = 10000
	// TokenTTL is the challenge lifetime at issuance.
	TokenTTL = 300 * time.Second
	// ExpLeeway is the verification-side grace on exp (spec §6 step 1).
	ExpLeeway = 30 * time.Second
	// BurnTTL is the Redis expiry of the burn key: TTL + leeway (spec §7).
	BurnTTL = 330 * time.Second
)

var (
	ErrBadToken = errors.New("malformed or tampered challenge")
	ErrExpired  = errors.New("challenge expired")
	ErrWrongSub = errors.New("challenge bound to another subject")
)

// Payload is the signed challenge payload (spec §5). Verification checks
// the exact signed bytes, so field order only matters at issuance.
type Payload struct {
	ID           string `json:"id"`
	Sub          string `json:"sub"`
	Iat          int64  `json:"iat"`
	Exp          int64  `json:"exp"`
	W0           uint64 `json:"w0"`
	L            uint32 `json:"l"`
	MinIntervalS uint32 `json:"min_interval_s"`
	MaxBatch     uint32 `json:"max_batch"`
}

// Sign serializes p and returns base64url(payloadJSON || HMAC-SHA256(payloadJSON, key)).
func Sign(p Payload, key []byte) (string, error) {
	body, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, key)
	mac.Write(body)
	return base64.RawURLEncoding.EncodeToString(append(body, mac.Sum(nil)...)), nil
}

// Parse decodes a token and verifies its HMAC against any of keys (dual-key
// rotation window: current, then previous). It does NOT check exp/sub — see
// Verify. Stateless: any replica verifies any token.
func Parse(token string, keys ...[]byte) (Payload, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(raw) <= sha256.Size {
		return Payload{}, ErrBadToken
	}
	body, sig := raw[:len(raw)-sha256.Size], raw[len(raw)-sha256.Size:]
	ok := false
	for _, key := range keys {
		if len(key) == 0 {
			continue
		}
		mac := hmac.New(sha256.New, key)
		mac.Write(body)
		if hmac.Equal(sig, mac.Sum(nil)) {
			ok = true
			break
		}
	}
	if !ok {
		return Payload{}, ErrBadToken
	}
	var p Payload
	if err := json.Unmarshal(body, &p); err != nil {
		return Payload{}, ErrBadToken
	}
	return p, nil
}

// Verify applies the semantic checks of spec §6 step 1: expiry (with
// leeway) and subject binding.
func Verify(p Payload, sub string, now time.Time) error {
	if now.After(time.Unix(p.Exp, 0).Add(ExpLeeway)) {
		return ErrExpired
	}
	if p.Sub == "" || p.Sub != sub {
		return ErrWrongSub
	}
	return nil
}
