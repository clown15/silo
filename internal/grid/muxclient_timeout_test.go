// Copyright (c) 2026 Feng Ruohang
//
// This file is part of Silo Object Storage.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package grid

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestSingleMutationWaitsBeyondDefaultTimeout(t *testing.T) {
	for _, admitted := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "admitted"}[admitted], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := t.Context()
				if admitted {
					ctx = WithoutDefaultTimeout(ctx)
				}
				conn := &Connection{outQueue: make(chan []byte, 1)}
				client := newMuxClient(ctx, 1, conn)
				defer client.cancelFn(context.Canceled)
				done := make(chan Response, 1)
				go func() {
					msg, err := client.roundtrip(handlerTest, nil)
					done <- Response{Msg: msg, Err: err}
				}()
				wire := <-conn.outQueue
				var request message
				if _, err := request.UnmarshalMsg(wire); err != nil {
					t.Fatal(err)
				}
				PutByteBuffer(wire)
				if admitted && request.DeadlineMS != 0 {
					t.Errorf("admitted mutation tells the server to discard a late result after %dms", request.DeadlineMS)
				}
				time.Sleep(defaultSingleRequestTimeout + time.Second)
				synctest.Wait()
				if !admitted {
					select {
					case got := <-done:
						if !errors.Is(got.Err, context.DeadlineExceeded) {
							t.Fatalf("ordinary timeout: %v", got.Err)
						}
					default:
						t.Fatal("ordinary request lost its default timeout")
					}
					return
				}
				select {
				case got := <-done:
					t.Fatalf("mutation returned before its commit completed: %v", got.Err)
				default:
				}
				client.response(0, Response{Msg: []byte("committed")})
				got := <-done
				if got.Err != nil || string(got.Msg) != "committed" {
					t.Fatalf("lost late commit result: %q, %v", got.Msg, got.Err)
				}
			})
		})
	}
}

func TestWithoutDefaultTimeoutStillEndsOnDisconnectOrCancellation(t *testing.T) {
	for _, disconnect := range []bool{false, true} {
		t.Run(map[bool]string{false: "explicit-cancel", true: "disconnect"}[disconnect], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				conn := &Connection{outQueue: make(chan []byte, 1)}
				client := newMuxClient(WithoutDefaultTimeout(ctx), 1, conn)
				defer client.cancelFn(context.Canceled)
				done := make(chan error, 1)
				go func() {
					_, err := client.roundtrip(handlerTest, nil)
					done <- err
				}()
				PutByteBuffer(<-conn.outQueue)
				want := context.Canceled
				if disconnect {
					client.close()
					want = ErrDisconnected
				} else {
					cancel()
				}
				if got := <-done; !errors.Is(got, want) {
					t.Fatalf("request did not terminate correctly: want %v, got %v", want, got)
				}
			})
		})
	}
}
