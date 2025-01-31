// Copyright 2017 Keybase, Inc. All rights reserved. Use of
// this source code is governed by the included BSD license.

package rpc

import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestPrioritizedRoundRobinRemote(t *testing.T) {
	_, err := ParsePrioritizedRoundRobinRemote(";,,;")
	require.Error(t, err)

	r, err := NewPrioritizedRoundRobinRemote([][]string{
		{}, // should be ignored
		{"a0", "a1", "a2", "a3"},
		{"b0", "b1"},
	})
	require.NoError(t, err)
	require.Equal(t, "a0,a1,a2,a3;b0,b1", r.String())

	_, err = NewPrioritizedRoundRobinRemote(nil)
	require.Error(t, err)
	_, err = NewPrioritizedRoundRobinRemote([][]string{{}, {}})
	require.Error(t, err)

	r, err = ParsePrioritizedRoundRobinRemote(`
	;;
	A0 , A1,a2,a3;
	;;b0,
	b1;
	`)
	require.NoError(t, err)

	// Fetch the address by calling the Peek and GetAddress methods in
	// parallel. Regression for a missing lock on Peek.
	getParallel := func() string {
		var g errgroup.Group
		g.Go(func() error {
			r.Peek()
			return nil
		})
		var getAddr string
		g.Go(func() error {
			getAddr = r.GetAddress()
			return nil
		})
		require.NoError(t, g.Wait())
		return getAddr
	}

	getAndConfirmA := func() {
		seen := make(map[string]bool)
		for i := 0; i < 4; i++ {
			seen[getParallel()] = true
		}

		require.True(t, seen["a0"])
		require.True(t, seen["a1"])
		require.True(t, seen["a2"])
		require.True(t, seen["a3"])
	}

	getAndConfirmB := func() {
		seen := make(map[string]bool)
		for i := 0; i < 2; i++ {
			seen[getParallel()] = true
		}

		require.True(t, seen["b0"])
		require.True(t, seen["b1"])
	}

	getAndConfirmA()
	getAndConfirmB()
	getAndConfirmA()
	r.Reset()
	getAndConfirmA()
	getAndConfirmB()
}
