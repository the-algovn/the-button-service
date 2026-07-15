package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	buttonv1 "github.com/the-algovn/protos/gen/go/algovn/button/v1"
)

type stubTotaler struct {
	total, users         uint64
	haveTotal, haveUsers bool
}

func (s stubTotaler) Total() (uint64, bool) { return s.total, s.haveTotal }
func (s stubTotaler) Users() (uint64, bool) { return s.users, s.haveUsers }

func TestGetCounter_ReturnsTotalAndUsers(t *testing.T) {
	s := &Server{Tick: stubTotaler{total: 1_204_882, users: 84_201, haveTotal: true, haveUsers: true}}
	resp, err := s.GetCounter(context.Background(), &buttonv1.GetCounterRequest{})
	require.NoError(t, err)
	require.Equal(t, uint64(1_204_882), resp.GetTotal())
	require.Equal(t, uint64(84_201), resp.GetTotalUsers())
}

func TestGetCounter_UnavailableUntilWarmed(t *testing.T) {
	s := &Server{Tick: stubTotaler{haveTotal: false}}
	_, err := s.GetCounter(context.Background(), &buttonv1.GetCounterRequest{})
	require.Equal(t, codes.Unavailable, status.Code(err))
}
