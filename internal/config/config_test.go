package config

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/require"
)

func setRequired(t *testing.T) {
	t.Helper()
	t.Setenv("PG_URL", "postgres://u:p@localhost:5432/the_button")
	t.Setenv("REDIS_URL", "redis://:pw@localhost:6379/0")
	t.Setenv("POW_SECRET", base64.StdEncoding.EncodeToString(make([]byte, 32)))
	t.Setenv("KAFKA_BROKERS", "localhost:9092")
}

func TestLoad_KafkaBrokers(t *testing.T) {
	setRequired(t)
	t.Setenv("KAFKA_BROKERS", "a:9092,b:9092")
	c, err := Load()
	require.NoError(t, err)
	require.Equal(t, []string{"a:9092", "b:9092"}, c.KafkaBrokers)
}

func TestLoad_DefaultsAndDecoding(t *testing.T) {
	setRequired(t)
	c, err := Load()
	require.NoError(t, err)
	require.Equal(t, ":9090", c.ListenAddr)
	require.Equal(t, ":9091", c.MetricsAddr)
	require.Equal(t, uint64(16384), c.PowW0)
	require.Len(t, c.PowSecret, 32)
	require.Nil(t, c.PowSecretPrev)
}

func TestLoad_MissingRequired(t *testing.T) {
	for _, missing := range []string{"PG_URL", "REDIS_URL", "POW_SECRET", "KAFKA_BROKERS"} {
		t.Run(missing, func(t *testing.T) {
			setRequired(t)
			t.Setenv(missing, "")
			_, err := Load()
			require.ErrorContains(t, err, missing)
		})
	}
}

func TestLoad_PrevKeyAndW0(t *testing.T) {
	setRequired(t)
	t.Setenv("POW_SECRET_PREV", base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef")))
	t.Setenv("POW_W0", "4096")
	c, err := Load()
	require.NoError(t, err)
	require.Len(t, c.PowSecretPrev, 32)
	require.Equal(t, uint64(4096), c.PowW0)
}

func TestLoad_BadBase64Secret(t *testing.T) {
	setRequired(t)
	t.Setenv("POW_SECRET", "not-base64!!!")
	_, err := Load()
	require.ErrorContains(t, err, "POW_SECRET")
}

func TestLoad_ShortSecret(t *testing.T) {
	setRequired(t)
	t.Setenv("POW_SECRET", base64.StdEncoding.EncodeToString(make([]byte, 16)))
	_, err := Load()
	require.ErrorContains(t, err, "POW_SECRET")
}

func TestLoad_ShortSecretPrev(t *testing.T) {
	setRequired(t)
	t.Setenv("POW_SECRET_PREV", base64.StdEncoding.EncodeToString(make([]byte, 16)))
	_, err := Load()
	require.ErrorContains(t, err, "POW_SECRET_PREV")
}

func TestLoadWorker_RequiredAndDefaults(t *testing.T) {
	t.Setenv("PG_URL", "postgres://u:p@localhost:5432/the_button")
	t.Setenv("REDIS_URL", "redis://:pw@localhost:6379/0")
	t.Setenv("KAFKA_BROKERS", "b1:9092, b2:9092")
	c, err := LoadWorker()
	require.NoError(t, err)
	require.Equal(t, ":9091", c.MetricsAddr)
	require.Equal(t, []string{"b1:9092", "b2:9092"}, c.KafkaBrokers)

	for _, missing := range []string{"PG_URL", "REDIS_URL", "KAFKA_BROKERS"} {
		t.Run(missing, func(t *testing.T) {
			t.Setenv("PG_URL", "x")
			t.Setenv("REDIS_URL", "x")
			t.Setenv("KAFKA_BROKERS", "x")
			t.Setenv(missing, "")
			_, err := LoadWorker()
			require.ErrorContains(t, err, missing)
		})
	}
}
