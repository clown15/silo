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

package cmd

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Keep the old slot calculation here to reproduce the false sharing between
// distinct resources, including resources on separate storage drives.
func mutationGuardLegacySlot(drive, path string) uint64 {
	h := uint64(14695981039346656037)
	for _, part := range []string{drive, path} {
		for i := range part {
			h = (h ^ uint64(part[i])) * 1099511628211
		}
		h *= 1099511628211
	}
	return h % 65536
}

func mutationGuardCollision(t *testing.T, drive string, target uint64) string {
	t.Helper()
	for i := 0; i < 2000000; i++ {
		key := fmt.Sprintf("independent-%d", i)
		if mutationGuardLegacySlot(drive, pathJoin("bucket", key)) == target {
			return key
		}
	}
	t.Fatal("no collision found in bounded search")
	return ""
}

func TestMutationGuardIsolatesStorageResources(t *testing.T) {
	blocked, rootA, err := newXLStorageTestSetup(t)
	if err != nil {
		t.Fatal(err)
	}
	defer blocked.Close()
	healthy, rootB, err := newXLStorageTestSetup(t)
	if err != nil {
		t.Fatal(err)
	}
	defer healthy.Close()
	defer mutationGuardCheckEmpty(t, rootA, rootB)
	for _, disk := range []*xlStorageDiskIDCheck{blocked, healthy} {
		if err := disk.MakeVol(t.Context(), "bucket"); err != nil {
			t.Fatal(err)
		}
	}
	const object = "blocked"
	target := mutationGuardLegacySlot(rootA, pathJoin("bucket", object))
	type mutation struct {
		name, key string
		disk      *xlStorageDiskIDCheck
		done      chan error
	}
	mutations := []mutation{
		{name: "same-key", key: object, disk: blocked},
		{name: "different-key-same-drive-collision", key: mutationGuardCollision(t, rootA, target), disk: blocked},
		{name: "different-key-healthy-drive-collision", key: mutationGuardCollision(t, rootB, target), disk: healthy},
		{name: "same-path-healthy-drive", key: object, disk: healthy},
	}
	fixture := func(name string) FileInfo {
		fi := newFileInfo(name, 2, 2)
		fi.DataDir, fi.ModTime, fi.Erasure.Index = mustGetUUID(), UTCNow(), 1
		fi.Fresh = true
		fi.Metadata = map[string]string{"x-amz-meta-probe": "before"}
		return fi
	}
	for _, m := range mutations {
		if err := m.disk.WriteMetadata(t.Context(), "", "bucket", m.key, fixture(m.key)); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var lastSuccess int64
	gate := &stagedRenameContext{
		Context: ctx, meta: filepath.Join(rootA, minioMetaTmpBucket, "staged", xlStorageFormatFile),
		entered: make(chan struct{}), release: make(chan struct{}),
		tracker: healthDiskCtxValue{lastSuccess: &lastSuccess},
	}
	var once sync.Once
	release := func() { once.Do(func() { close(gate.release) }) }
	var workers sync.WaitGroup
	defer func() {
		release()
		workers.Wait()
	}()
	holderDone := make(chan error, 1)
	workers.Go(func() {
		_, err := blocked.RenameData(gate, minioMetaTmpBucket, "staged", fixture(object), "bucket", object, RenameOptions{})
		holderDone <- err
	})
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("RenameData did not reach the staged metadata barrier")
	}
	// Cancellation cannot release the guard while the admitted worker can
	// still commit. Unrelated mutations must nevertheless be able to finish.
	cancel()
	for i := range mutations {
		m := &mutations[i]
		m.done = make(chan error, 1)
		workers.Go(func() {
			m.done <- m.disk.UpdateMetadata(t.Context(), "bucket", m.key, FileInfo{
				Metadata: map[string]string{"x-amz-meta-probe": "after"},
			}, UpdateMetadataOpts{})
		})
	}
	mutationGuardWaitRefs(t, rootA, pathJoin("bucket", object), 2)
	for _, m := range mutations[1:] {
		select {
		case err := <-m.done:
			if err != nil {
				t.Errorf("%s: %v", m.name, err)
			}
			t.Logf("%s completed while RenameData remained paused", m.name)
		case <-time.After(2 * time.Second):
			t.Errorf("%s blocked by an unrelated paused mutation", m.name)
		}
	}
	select {
	case err := <-holderDone:
		t.Fatalf("canceled RenameData returned before its worker finished: %v", err)
	default:
	}
	select {
	case err := <-mutations[0].done:
		t.Fatalf("same-key mutation escaped the held guard: %v", err)
	default:
	}
	release()
	select {
	case err := <-holderDone:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("canceled RenameData result: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RenameData did not finish after releasing the barrier")
	}
	select {
	case err := <-mutations[0].done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("same-key mutation did not finish after releasing the holder")
	}
	workers.Wait()
	for _, m := range mutations {
		fi, err := m.disk.ReadVersion(t.Context(), "", "bucket", m.key, "", ReadOptions{})
		if err != nil || fi.Metadata["x-amz-meta-probe"] != "after" {
			t.Errorf("%s persisted metadata: %q, %v", m.name, fi.Metadata["x-amz-meta-probe"], err)
		}
	}
}

func mutationGuardCheckEmpty(t *testing.T, drives ...string) {
	t.Helper()
	storageMutationLocks.Lock()
	defer storageMutationLocks.Unlock()
	for key, lock := range storageMutationLocks.locks {
		if slices.Contains(drives, key.drive) {
			t.Errorf("unused guard retained for %+v with %d references", key, lock.refs)
		}
	}
}

func mutationGuardWaitRefs(t *testing.T, drive, path string, refs int) *storageMutationLock {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	key := storageMutationKey{drive, path}
	for {
		storageMutationLocks.Lock()
		lock := storageMutationLocks.locks[key]
		ready := lock != nil && lock.refs == refs
		storageMutationLocks.Unlock()
		if ready {
			return lock
		}
		select {
		case <-deadline.C:
			t.Fatalf("guard %+v did not reach %d references", key, refs)
		case <-tick.C:
		}
	}
}

func TestMutationGuardRetainsWaitersAndRemovesIdleEntries(t *testing.T) {
	s := &xlStorage{drivePath: t.TempDir()}
	defer mutationGuardCheckEmpty(t, s.drivePath)
	unlock := sync.OnceFunc(s.lockMutations("object"))
	defer unlock()
	original := mutationGuardWaitRefs(t, s.drivePath, "object", 1)
	acquired, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	go func() {
		defer close(finished)
		defer s.lockMutations("object", "object")()
		close(acquired)
		<-release
	}()
	if lock := mutationGuardWaitRefs(t, s.drivePath, "object", 2); lock != original {
		t.Error("waiting operation registered a second mutex for the same resource")
	}
	select {
	case <-acquired:
		t.Error("waiter acquired before the holder released")
	default:
	}
	unlock()
	select {
	case <-acquired:
	case <-time.After(5 * time.Second):
		t.Fatal("waiter did not acquire after release")
	}
	if lock := mutationGuardWaitRefs(t, s.drivePath, "object", 1); lock != original {
		t.Error("holder release discarded the queued operation's mutex")
	}
	unblock()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("waiter did not release")
	}
	mutationGuardCheckEmpty(t, s.drivePath)
	// The registry must not grow with the historical object count.
	for i := 0; i < 1000; i++ {
		s.lockMutations(fmt.Sprintf("object-%d", i))()
	}
	s.lockMutations()()
}

func TestMutationGuardOrdersAndDeduplicatesPaths(t *testing.T) {
	s := &xlStorage{drivePath: t.TempDir()}
	defer mutationGuardCheckEmpty(t, s.drivePath)
	paths := []string{"b", "a", "b", "a"}
	before := slices.Clone(paths)
	unlock := s.lockMutations(paths...)
	mutationGuardWaitRefs(t, s.drivePath, "a", 1)
	mutationGuardWaitRefs(t, s.drivePath, "b", 1)
	unlock()
	if !slices.Equal(paths, before) {
		t.Fatalf("changed caller's path slice: %v", paths)
	}
	const workers, iterations = 8, 500
	start, done := make(chan struct{}), make(chan struct{})
	var wg sync.WaitGroup
	var protected atomic.Int64
	var active atomic.Int32
	for i := 0; i < workers; i++ {
		worker := i
		wg.Go(func() {
			<-start
			order := []string{"a", "b", "a"}
			if worker%2 != 0 {
				order = []string{"b", "a", "b"}
			}
			for j := 0; j < iterations; j++ {
				unlock := s.lockMutations(order...)
				if active.Add(1) != 1 {
					t.Error("overlapping guards entered concurrently")
				}
				protected.Add(1)
				active.Add(-1)
				unlock()
			}
		})
	}
	go func() { wg.Wait(); close(done) }()
	close(start)
	select {
	case <-done:
		if got := protected.Load(); got != workers*iterations {
			t.Fatalf("overlapping guards lost updates: got %d, want %d", got, workers*iterations)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("opposing path orders or duplicate paths deadlocked")
	}
}

// Measure registry/allocation cost separately from storage I/O. Independent
// workers use two resources, as RenameData does, and shared workers use one.
func BenchmarkMutationGuard(b *testing.B) {
	s := &xlStorage{drivePath: "/benchmark/drive"}
	b.Run("single", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			s.lockMutations("bucket/object")()
		}
	})
	b.Run("parallel-independent", func(b *testing.B) {
		b.ReportAllocs()
		var worker atomic.Uint64
		b.RunParallel(func(pb *testing.PB) {
			id := worker.Add(1)
			paths := []string{fmt.Sprintf("staging/%d", id), fmt.Sprintf("bucket/object-%d", id)}
			for pb.Next() {
				s.lockMutations(paths...)()
			}
		})
	})
	b.Run("parallel-shared", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				s.lockMutations("bucket/object")()
			}
		})
	})
}

// A call must retain paths it has not reached yet, even across xlStorage
// instances for the same drive. No resource wait may hold the registry mutex.
func TestMutationGuardRetainsUnacquiredPaths(t *testing.T) {
	s := &xlStorage{drivePath: t.TempDir()}
	sameDrive := &xlStorage{drivePath: s.drivePath}
	defer mutationGuardCheckEmpty(t, s.drivePath)
	unlockA := sync.OnceFunc(s.lockMutations("a"))
	defer unlockA()
	acquired, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	go func() {
		defer close(done)
		defer sameDrive.lockMutations("b", "a")()
		close(acquired)
		<-release
	}()
	mutationGuardWaitRefs(t, s.drivePath, "a", 2)
	original := mutationGuardWaitRefs(t, s.drivePath, "b", 1)
	// The queued call has not locked b yet: sorting must put a first.
	unlockB := sync.OnceFunc(s.lockMutations("b"))
	defer unlockB()
	if got := mutationGuardWaitRefs(t, s.drivePath, "b", 2); got != original {
		t.Fatal("unacquired path got a different mutex")
	}
	unlockA()
	select {
	case <-acquired:
		t.Fatal("multi-path call escaped the independently held second path")
	default:
	}
	unlockB()
	select {
	case <-acquired:
	case <-time.After(5 * time.Second):
		t.Fatal("multi-path call did not acquire after both paths were released")
	}
	if got := mutationGuardWaitRefs(t, s.drivePath, "b", 1); got != original {
		t.Fatal("release replaced an entry retained by a pending multi-path call")
	}
	unblock()
	<-done
	// The variadic helper has no hidden four-path limit.
	s.lockMutations("e", "d", "c", "b", "a", "a")()
}

func TestMutationGuardCanceledWaiterRetainsSerialization(t *testing.T) {
	disk, root, err := newXLStorageTestSetup(t)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()
	defer mutationGuardCheckEmpty(t, root)
	if err := disk.MakeVol(t.Context(), "bucket"); err != nil {
		t.Fatal(err)
	}
	fi := newFileInfo("object", 2, 2)
	fi.DataDir, fi.ModTime, fi.Erasure.Index = mustGetUUID(), UTCNow(), 1
	fi.Fresh = true
	fi.Metadata = map[string]string{"x-amz-meta-probe": "before"}
	if err := disk.WriteMetadata(t.Context(), "", "bucket", "object", fi); err != nil {
		t.Fatal(err)
	}
	// Use a separate instance with the same drive identity as the API wrapper.
	s := &xlStorage{drivePath: root}
	unlock := sync.OnceFunc(s.lockMutations("bucket/object"))
	defer unlock()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- disk.UpdateMetadata(ctx, "bucket", "object", FileInfo{
			Metadata: map[string]string{"x-amz-meta-probe": "canceled"},
		}, UpdateMetadataOpts{})
	}()
	mutationGuardWaitRefs(t, root, "bucket/object", 2)
	cancel()
	select {
	case err := <-done:
		t.Fatalf("canceled API returned with worker still queued: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	mutationGuardWaitRefs(t, root, "bucket/object", 2)
	unlock()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled waiter: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled waiter did not finish after holder release")
	}
	got, err := disk.ReadVersion(t.Context(), "", "bucket", "object", "", ReadOptions{})
	if err != nil || got.Metadata["x-amz-meta-probe"] != "before" {
		t.Fatalf("canceled metadata update persisted: %v, %v", got.Metadata, err)
	}
}
