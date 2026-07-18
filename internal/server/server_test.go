package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

// authCtx forges the forwarded (gateway-verified) JWT: only segment 2 is
// read by the trust-model decode. Shared with the integration tests.
func authCtx(sub string) context.Context {
	payload, _ := json.Marshal(map[string]string{"sub": sub, "name": sub + "-name"})
	tok := "h." + base64.RawURLEncoding.EncodeToString(payload) + ".s"
	return metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer "+tok))
}

func TestSubFromContext(t *testing.T) {
	sub, err := subFromContext(authCtx("zitadel-user-42"))
	require.NoError(t, err)
	require.Equal(t, "zitadel-user-42", sub)
}

func TestSubFromContext_Rejects(t *testing.T) {
	// no metadata
	_, err := subFromContext(context.Background())
	require.Error(t, err)
	// not a JWT
	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer nope"))
	_, err = subFromContext(ctx)
	require.Error(t, err)
	// bad segment-2 base64
	ctx = metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer h.!!!.s"))
	_, err = subFromContext(ctx)
	require.Error(t, err)
	// empty sub claim
	ctx = metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer h."+base64.RawURLEncoding.EncodeToString([]byte(`{}`))+".s"))
	_, err = subFromContext(ctx)
	require.Error(t, err)
}
