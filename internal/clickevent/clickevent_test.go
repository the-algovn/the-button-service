package clickevent

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRoundTrip(t *testing.T) {
	c := Click{Sub: "u1", Count: 7, ChallengeID: "ch-1", TsUnix: 123, DisplayName: "Nyx"}
	b, err := c.Marshal()
	require.NoError(t, err)
	got, err := Unmarshal(b)
	require.NoError(t, err)
	require.Equal(t, c, got)
	require.Equal(t, []byte("u1"), c.Key())
}

func TestUnmarshalRejectsGarbage(t *testing.T) {
	_, err := Unmarshal([]byte("not json"))
	require.Error(t, err)
}
