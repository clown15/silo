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
	"bytes"
	"context"
	"github.com/minio/minio/internal/cachevalue"
	"github.com/minio/minio/internal/config/drive"
	xhttp "github.com/minio/minio/internal/http"
	xioutil "github.com/minio/minio/internal/ioutil"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func mutationPools(t testing.TB) (*erasureServerPools, string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	dirs, err := getRandomDisks(8)
	if err != nil {
		t.Fatal(err)
	}
	endpoints := mustGetPoolEndpoints(0, dirs[:8]...)

	obj, _, err := initObjectLayer(ctx, endpoints)
	if err != nil {
		cancel()
		removeRoots(dirs)
		t.Fatal(err)
	}
	z := obj.(*erasureServerPools)
	old := newObjectLayerFn()
	setObjectLayer(z)
	t.Cleanup(func() { cancel(); z.Shutdown(context.Background()); removeRoots(dirs); setObjectLayer(old) })
	bucket := "scope-probe"
	if err := z.MakeBucket(ctx, bucket, MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	return z, bucket
}
func mutationPut(t testing.TB, z *erasureServerPools, bucket, obj string, pool int, body string, when time.Time) {
	t.Helper()
	_, err := z.serverPools[pool].PutObject(context.Background(), bucket, obj, mustGetPutObjReader(t, bytes.NewBufferString(body), int64(len(body)), "", ""), ObjectOptions{MTime: when})
	if err != nil {
		t.Fatal(err)
	}
}

// Ordinary successful tag write with one temporarily unavailable drive. No
// metadata is fabricated. The stale drive returns first on a subsequent GET.
func TestMutationReadPreservesAcknowledgedTags(t *testing.T) {
	z, bucket := mutationPools(t)
	const object = "tag-read"
	mutationPut(t, z, bucket, object, 0, "payload", time.Now())
	set := z.serverPools[0].getHashedSet(object)
	disks, swap := installListingDiskSwitch(set)
	defer swap(disks)
	partial := append([]StorageAPI(nil), disks...)
	partial[0] = nil
	swap(partial)
	const tags = "owner=acknowledged-B"
	if _, err := set.PutObjectTags(t.Context(), bucket, object, tags, ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	swap(disks)
	before, err := disks[0].ReadVersion(t.Context(), "", bucket, object, "", ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	after, err := disks[1].ReadVersion(t.Context(), "", bucket, object, "", ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("old/new tags=%q/%q; equal tuple=%t", before.Metadata[xhttp.AmzObjectTagging], after.Metadata[xhttp.AmzObjectTagging], before.ModTime.Equal(after.ModTime) && before.DataDir == after.DataDir && before.Metadata["etag"] == after.Metadata["etag"])
	oi, err := set.GetObjectInfo(t.Context(), bucket, object, ObjectOptions{FastGetObjInfo: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("successful fast GET metadata returned tags=%q", oi.UserTags)
	if oi.UserTags != tags {
		t.Fatalf("acknowledged tags were replaced by stale metadata despite matching ModTime/DataDir/ETag: %q", oi.UserTags)
	}
}

type mutationAmbiguousRename struct {
	StorageAPI
	index   int
	entered chan<- struct{}
	release <-chan struct{}
}

func (d mutationAmbiguousRename) RenameData(ctx context.Context, sv, sp string, fi FileInfo, dv, dp string, o RenameOptions) (RenameDataResp, error) {
	if d.index >= 5 {
		return RenameDataResp{}, errDiskNotFound
	}
	r, e := d.StorageAPI.RenameData(ctx, sv, sp, fi, dv, dp, o)
	d.entered <- struct{}{}
	<-d.release
	if d.index == 4 && e == nil {
		return r, context.DeadlineExceeded
	}
	return r, e
}

// A physical fifth commit loses its reply. B successfully updates tags on all
// five copies of A. A then undoes four known successes. B's tag update retains
// the DataDir and ModTime, so the proposed ownership predicate cannot see it.
func TestMutationRollbackPreservesAcknowledgedTags(t *testing.T) {
	z, bucket := mutationPools(t)
	const object = "tag-undo"
	base := time.Now().Add(-time.Hour)
	mutationPut(t, z, bucket, object, 0, "initial", base)
	set := z.serverPools[0].getHashedSet(object)
	disks, swap := installListingDiskSwitch(set)
	defer swap(disks)
	entered, release := make(chan struct{}, 5), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	wrapped := append([]StorageAPI(nil), disks...)
	for i, d := range disks {
		wrapped[i] = mutationAmbiguousRename{d, i, entered, release}
	}
	swap(wrapped)
	aDone := make(chan error, 1)
	go func() {
		body := "pending-A"
		_, e := set.PutObject(t.Context(), bucket, object, mustGetPutObjReader(t, strings.NewReader(body), int64(len(body)), "", ""), ObjectOptions{NoLock: true, MTime: base.Add(time.Minute)})
		aDone <- e
	}()
	for range 5 {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("A did not finish physical commits")
		}
	}
	swap(disks)
	const tags = "owner=acknowledged-B"
	if _, err := set.PutObjectTags(t.Context(), bucket, object, tags, ObjectOptions{NoLock: true}); err != nil {
		t.Fatalf("B tags failed: %v", err)
	}
	count := func() int {
		n := 0
		for _, d := range disks {
			fi, e := d.ReadVersion(t.Context(), "", bucket, object, "", ReadOptions{})
			if e == nil && fi.Metadata[xhttp.AmzObjectTagging] == tags {
				n++
			}
		}
		return n
	}
	if n := count(); n != 5 {
		t.Fatalf("B did not update all five A copies: %d", n)
	}
	t.Log("B tag update acknowledged on five physical copies; fifth A reply will report timeout")
	unblock()
	select {
	case e := <-aDone:
		if e == nil {
			t.Fatal("A unexpectedly succeeded")
		}
		t.Logf("A result=%v", e)
	case <-time.After(5 * time.Second):
		t.Fatal("A did not finish")
	}
	if n := count(); n != 5 {
		t.Errorf("acknowledged tags lost physical copies: got %d, want 5", n)
	}
	oi, e := set.GetObjectInfo(t.Context(), bucket, object, ObjectOptions{})
	if e != nil {
		t.Fatal(e)
	}
	if oi.UserTags != tags {
		t.Fatalf("version-scoped rollback lost acknowledged tag update: tags=%q", oi.UserTags)
	}
}

type reviewPartialRename struct {
	StorageAPI
	fail    bool
	entered chan<- struct{}
	release <-chan struct{}
}

func (d reviewPartialRename) RenameData(ctx context.Context, sv, sp string, fi FileInfo, dv, dp string, opts RenameOptions) (RenameDataResp, error) {
	if d.fail {
		return RenameDataResp{}, errDiskNotFound
	}
	r, err := d.StorageAPI.RenameData(ctx, sv, sp, fi, dv, dp, opts)
	d.entered <- struct{}{}
	<-d.release // Delay the reply, after the physical mutation and local unlock.
	return r, err
}

// One pool, eight real disk directories, no edited object metadata. A commits
// on four disks (below write quorum five); its replies are delayed. Model a lost
// namespace lease by letting B execute with NoLock, like the previous lease
// fixture. B adds an explicit version and succeeds on seven disks. A then
// fails quorum and rolls back. A must not erase the separately acknowledged B.
func TestMutationRollbackPreservesConcurrentVersion(t *testing.T) {
	z, bucket := mutationPools(t)
	const object = "rollback-and-version"
	size, _ := strconv.Atoi(os.Getenv("SILO_REVIEW_BYTES"))
	payload := func(v string) string {
		if size > len(v) {
			return v + strings.Repeat(".", size-len(v))
		}
		return v
	}
	base := time.Now().Add(-time.Hour)
	mutationPut(t, z, bucket, object, 0, payload("initial-null"), base)
	set := z.serverPools[0].getHashedSet(object)
	disks, swap := installListingDiskSwitch(set)
	defer swap(disks)
	entered, release := make(chan struct{}, 4), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	aDisks := append([]StorageAPI(nil), disks...)
	for i, d := range disks {
		aDisks[i] = reviewPartialRename{d, i >= 4, entered, release}
	}
	swap(aDisks)
	aDone := make(chan error, 1)
	go func() {
		body := payload("pending-null-A")
		_, err := z.serverPools[0].PutObject(t.Context(), bucket, object,
			mustGetPutObjReader(t, strings.NewReader(body), int64(len(body)), "", ""),
			ObjectOptions{NoLock: true, MTime: base.Add(time.Minute)})
		aDone <- err
	}()
	for range 4 {
		select {
		case <-entered:
		case err := <-aDone:
			t.Fatalf("A returned early: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("A did not reach the reply barrier")
		}
	}
	t.Log("A physically committed on four disks; replies held; other four RenameData calls failed")
	bDisks := append([]StorageAPI(nil), disks...)
	bDisks[7] = nil
	swap(bDisks)
	bVersion := mustGetUUID()
	body := payload("acknowledged-version-B")
	_, err := z.serverPools[0].PutObject(t.Context(), bucket, object,
		mustGetPutObjReader(t, strings.NewReader(body), int64(len(body)), "", ""),
		ObjectOptions{NoLock: true, Versioned: true, VersionID: bVersion, MTime: base.Add(2 * time.Minute)})
	if err != nil {
		t.Fatalf("B failed: %v", err)
	}
	countB := func() int {
		n := 0
		for _, d := range disks {
			if _, e := d.ReadVersion(t.Context(), "", bucket, object, bVersion, ReadOptions{}); e == nil {
				n++
			}
		}
		return n
	}
	if n := countB(); n != 7 {
		t.Fatalf("B was not committed on the expected seven drives: %d", n)
	}
	t.Log("B succeeded on seven drives and its version is present on all seven")
	unblock()
	select {
	case err := <-aDone:
		if err == nil {
			t.Fatal("A unexpectedly succeeded")
		}
		t.Logf("A failed and rolled back: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("A did not finish")
	}
	swap(disks)
	t.Logf("B metadata survives on %d of eight drives after A's rollback", countB())
	r, err := z.GetObjectNInfo(t.Context(), bucket, object, nil, nil, ObjectOptions{VersionID: bVersion})
	if err != nil {
		t.Fatalf("acknowledged B became unreadable after unrelated rollback: %v", err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil || !bytes.Equal(got, []byte(body)) {
		t.Fatalf("B payload changed: readErr=%v length=%d", err, len(got))
	}
}

func TestMutationDeleteMarkerDoesNotReenter(t *testing.T) {
	disk, _, err := newXLStorageTestSetup(t)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()
	if err := disk.MakeVol(t.Context(), "bucket"); err != nil {
		t.Fatal(err)
	}
	fi := FileInfo{Volume: "bucket", Name: "absent", VersionID: mustGetUUID(), Deleted: true, ModTime: UTCNow()}
	done := make(chan error, 1)
	go func() { done <- disk.storage.DeleteVersion(t.Context(), "bucket", "absent", fi, true, DeleteOptions{}) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("DeleteVersion(forceDelMarker) blocked acquiring the same guard again through WriteMetadata")
	}
}

type reviewReadGate struct {
	StorageAPI
	ready   chan<- struct{}
	release <-chan struct{}
}

func (d reviewReadGate) ReadXL(ctx context.Context, volume, path string, data bool) (RawFileInfo, error) {
	r, err := d.StorageAPI.ReadXL(ctx, volume, path, data)
	d.ready <- struct{}{}
	if d.release != nil {
		<-d.release
	}
	return r, err
}

func TestReviewFastReadAlreadyWaitsForSlowDisk(t *testing.T) {
	z, bucket := mutationPools(t)
	const object = "healthy-slow-read"
	mutationPut(t, z, bucket, object, 0, "healthy-inline", time.Now())
	set := z.serverPools[0].getHashedSet(object)
	disks, swap := installListingDiskSwitch(set)
	defer swap(disks)
	ready, release := make(chan struct{}, len(disks)), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	wrapped := append([]StorageAPI(nil), disks...)
	for i, d := range disks {
		g := reviewReadGate{StorageAPI: d, ready: ready}
		if i == len(disks)-1 {
			g.release = release
		}
		wrapped[i] = g
	}
	swap(wrapped)
	done := make(chan error, 1)
	go func() {
		_, _, _, err := set.getObjectFileInfo(t.Context(), bucket, object, ObjectOptions{FastGetObjInfo: true}, true)
		done <- err
	}()
	for range disks {
		select {
		case <-ready:
		case <-time.After(5 * time.Second):
			t.Fatal("reads did not reach barrier")
		}
	}
	select {
	case err := <-done:
		t.Fatalf("read returned before the final disk: %v", err)
	case <-time.After(100 * time.Millisecond):
		t.Log("seven healthy replies are insufficient to return; final unbuffered mrfCheck send is still waiting")
	}
	unblock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("read did not finish after slow disk release")
	}
}

// Pause the real trash worker at its disk-space callback, after moving the
// external shards but before DeleteVersion can replace xl.meta. An outer join
// must also contain this nested worker, and later mutations must remain excluded.
func TestMutationDeleteJoinsNestedWorker(t *testing.T) {
	z, bucket := mutationPools(t)
	const object = "nested-timeout"
	mutationPut(t, z, bucket, object, 0, strings.Repeat("x", 2<<20), time.Now())
	disk := z.serverPools[0].getHashedSet(object).getDisks()[0].(*xlStorageDiskIDCheck)
	fi, err := disk.ReadVersion(t.Context(), "", bucket, object, "", ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	oldCache := disk.storage.diskInfoCache
	disk.storage.diskInfoCache = cachevalue.NewFromFunc(time.Hour, cachevalue.Opts{}, func(context.Context) (DiskInfo, error) {
		close(entered)
		<-release
		return DiskInfo{Used: 100, Free: 100, UsedInodes: 100, FreeInodes: 100}, nil
	})
	defer func() { disk.storage.diskInfoCache = oldCache }()
	oldTimeout := globalDriveConfig.GetMaxTimeout()
	globalDriveConfig.Update(drive.Config{MaxTimeout: 1200 * time.Millisecond})
	defer globalDriveConfig.Update(drive.Config{MaxTimeout: oldTimeout})
	done := make(chan error, 1)
	go func() {
		_, e := xioutil.WithDeadlineAndWait(context.Background(), globalDriveConfig.GetMaxTimeout(), func(ctx context.Context) (struct{}, error) {
			return struct{}{}, disk.storage.DeleteVersion(ctx, bucket, object, fi, false, DeleteOptions{UndoWrite: true})
		})
		done <- e
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("trash callback not reached")
	}
	early := false
	select {
	case e := <-done:
		early = true
		t.Errorf("outer join returned before nested disk worker completed: %v", e)
	case <-time.After(1500 * time.Millisecond):
	}
	updated := make(chan error, 1)
	go func() {
		updated <- disk.storage.UpdateMetadata(context.Background(), bucket, object, FileInfo{VersionID: fi.VersionID, Metadata: map[string]string{"after-timeout": "yes"}}, UpdateMetadataOpts{})
	}()
	writerEarly := false
	select {
	case e := <-updated:
		writerEarly = true
		t.Errorf("later metadata writer passed unfinished deletion: %v", e)
	case <-time.After(100 * time.Millisecond):
	}
	unblock()
	if !early {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("delete did not finish")
		}
	}
	if !writerEarly {
		select {
		case <-updated:
		case <-time.After(5 * time.Second):
			t.Fatal("later writer did not finish")
		}
	}
}
