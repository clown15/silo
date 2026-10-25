// Copyright (c) 2026 Feng Ruohang
//
// This file is part of Silo Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http"
	"slices"
	"testing"
	"time"
)

func TestListFallbackPreservesPoolDeleteMarker(t *testing.T) {
	for _, markerPool := range []int{0, 1} {
		t.Run(fmt.Sprint(markerPool), func(t *testing.T) {
			z, bucket, router := listingPoolsBackend(t)
			ctx := t.Context()
			live, deleted := z.serverPools[1-markerPool].sets[0], z.serverPools[markerPool].sets[0]
			disks, switchDisks := installListingDiskSwitch(deleted)
			const object = "gone"
			body := bytes.Repeat([]byte{'a'}, 8192)
			oldTime := time.Now().UTC().Add(-time.Hour)
			for _, set := range []*erasureObjects{live, deleted} {
				_, err := set.PutObject(ctx, bucket, object, mustGetPutObjReader(t, bytes.NewReader(body), int64(len(body)), "", ""), ObjectOptions{MTime: oldTime, VersionSuspended: true})
				if err != nil {
					t.Fatal(err)
				}
			}
			old := make([][]byte, 3)
			for i := range old {
				old[i] = mustReadNullQuorumMeta(t, disks[i], bucket, object)
			}
			_, err := deleted.DeleteObject(ctx, bucket, object, ObjectOptions{VersionSuspended: true, DeleteMarker: true, MTime: oldTime.Add(time.Minute)})
			if err != nil {
				t.Fatal(err)
			}
			get := nullQuorumRequest(t, router, http.MethodGet, getGetObjectURL("", bucket, object))
			if get.Code != http.StatusNotFound {
				t.Fatalf("expected deleted GET, got %d %s", get.Code, get.Body.String())
			}
			first := nullQuorumRequest(t, router, http.MethodGet, getListObjectsV2URL("", bucket, "", "1000", "", "", ""))
			var page ListObjectsV2Response
			if first.Code != http.StatusOK || xml.Unmarshal(first.Body.Bytes(), &page) != nil || len(page.Contents) != 0 {
				t.Fatalf("normal LIST marker: %d %s", first.Code, first.Body.String())
			}
			for i := range old {
				if err := disks[i].WriteAll(ctx, bucket, object+"/"+xlStorageFormatFile, old[i]); err != nil {
					t.Fatal(err)
				}
			}
			walkers := slices.Clone(disks)
			for i := 8; i < len(walkers); i++ {
				walkers[i] = &failedMetadataWalkDisk{StorageAPI: disks[i]}
			}
			switchDisks(walkers)
			fi, _, _, err := deleted.getObjectFileInfo(ctx, bucket, object, ObjectOptions{}, false)
			if err != nil || !fi.Deleted {
				t.Fatalf("read fallback marker: %+v %v", fi, err)
			}
			rec := nullQuorumRequest(t, router, http.MethodGet, getListObjectsV2URL("", bucket, "", "1000", "", "", ""))
			page = ListObjectsV2Response{}
			if rec.Code != http.StatusOK || xml.Unmarshal(rec.Body.Bytes(), &page) != nil || len(page.Contents) != 0 {
				t.Fatalf("fallback resurrected old pool copy: %d %s", rec.Code, rec.Body.String())
			}
		})
	}
}

// The regular metadata reader can translate a persistent generation split to
// ObjectNotFound even though every drive still has valid metadata. LIST must
// preserve that uncertainty instead of treating the key as deleted.
func TestListFallbackRejectsSplitGenerations(t *testing.T) {
	z, bucket, _ := nullQuorumBackend(t)
	set := z.serverPools[0].sets[0]
	disks := set.getDisks()
	const object = "object"
	mtime := time.Now().UTC().Add(-time.Hour)
	var generations [3][][]byte
	for generation := range generations {
		body := bytes.Repeat([]byte{byte('a' + generation)}, 8192)
		nullQuorumPut(t, z, bucket, object, body, ObjectOptions{MTime: mtime.Add(time.Duration(generation) * time.Minute)})
		generations[generation] = make([][]byte, len(disks))
		for i, disk := range disks {
			generations[generation][i] = mustReadNullQuorumMeta(t, disk, bucket, object)
		}
	}
	// All sixteen drives are online, but the 7/5/4 split leaves no generation
	// with the eight matching snapshots required by the listing resolver.
	for i, disk := range disks {
		generation := 0
		if i >= 7 {
			generation = 1
		}
		if i >= 12 {
			generation = 2
		}
		if err := disk.WriteAll(t.Context(), bucket, object+"/"+xlStorageFormatFile, generations[generation][i]); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, _, err := set.getObjectFileInfo(t.Context(), bucket, object, ObjectOptions{}, false); !isErrObjectNotFound(err) {
		t.Fatalf("split metadata did not exercise ambiguous not-found: %v", err)
	}
	entries := make(metaCacheEntries, len(disks))
	for i, disk := range disks {
		entries[i] = metaCacheEntry{name: object, metadata: mustReadNullQuorumMeta(t, disk, bucket, object)}
		if _, err := entries[i].xlmeta(); err != nil {
			t.Fatalf("drive %d no longer has valid metadata: %v", i, err)
		}
	}
	resolver := metadataResolutionParams{dirQuorum: 8, objQuorum: 8, bucket: bucket, requestedVersions: 1}
	entry, err := z.resolveListingGroup(t.Context(), bucket, []*metaCacheEntry{set.resolveListEntry(entries, nil, &resolver)})
	if entry != nil || !isErrReadQuorum(err) {
		t.Fatalf("split live metadata became absence: entry=%v err=%v", entry, err)
	}
}
