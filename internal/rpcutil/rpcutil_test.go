package rpcutil

import (
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestLeaderHint(t *testing.T) {
	err := status.Errorf(codes.FailedPrecondition, "not leader (leader=%q)", "10.0.0.1:8000")
	if !IsNotLeader(err) {
		t.Fatal("IsNotLeader")
	}
	if got := LeaderHint(nil, err); got != "10.0.0.1:8000" {
		t.Fatalf("hint from message: %q", got)
	}
	md := metadata.Pairs(LeaderTrailer, "10.0.0.2:8000")
	if got := LeaderHint(md, err); got != "10.0.0.2:8000" {
		t.Fatalf("hint from trailer: %q", got)
	}
	empty := status.Errorf(codes.FailedPrecondition, "not leader (leader=%q)", "")
	if got := LeaderHint(nil, empty); got != "" {
		t.Fatalf("empty hint: %q", got)
	}
	if IsNotLeader(errors.New("x")) || IsNotLeader(status.Error(codes.FailedPrecondition, "other")) {
		t.Fatal("false positive")
	}
	if !IsUnavailable(status.Error(codes.Unavailable, "")) || IsUnavailable(status.Error(codes.NotFound, "")) {
		t.Fatal("IsUnavailable")
	}
}
