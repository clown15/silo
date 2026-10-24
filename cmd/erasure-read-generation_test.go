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
	"fmt"
	"net/http"
	"testing"
	"time"
)

// All metadata is valid and readable. Only the completed generations differ.
// No timing, missing files, synthetic read error or malformed metadata is needed.
func TestReadGenerationConflictIsNotAbsence(t *testing.T) {
	raw := make([]RawFileInfo, 8)
	for i := range raw {
		fi := newFileInfo("object", 4, 4)
		fi.Erasure.Index = i + 1
		fi.ModTime = time.Unix(100+int64(i%4), 0)
		fi.DataDir = mustGetUUID()
		fi.Metadata = map[string]string{"etag": fmt.Sprint(i % 4)}
		var meta xlMetaV2
		if err := meta.AddVersion(fi); err != nil {
			t.Fatal(err)
		}
		buf, err := meta.AppendTo(nil)
		if err != nil {
			t.Fatal(err)
		}
		raw[i] = RawFileInfo{Buf: buf}
	}
	_, errs := pickLatestQuorumFilesInfo(t.Context(), raw, make([]error, len(raw)), "bucket", "object", true, false)
	err := reduceReadQuorumErrs(t.Context(), errs, objectOpIgnoredErrs, 4)
	if api := toAPIError(t.Context(), toObjectErr(err, "bucket", "object")); api.HTTPStatusCode != http.StatusServiceUnavailable {
		t.Fatalf("readable conflicting generations reported as absence: %v (%+v)", err, api)
	}
}

func TestPoolsReadGenerationConflictDoesNotExposeOldValue(t *testing.T) {
	z, bucket := consistencyPools(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	router := initTestAPIEndPoints(z, []string{"GetObject", "HeadObject"})
	for _, tc := range []struct {
		pool int
		mode string
	}{
		{0, "split"},
		{1, "split"},
		{0, "older-quorum"},
		{1, "older-quorum"},
		{0, "newer-quorum"},
		{1, "newer-quorum"},
	} {
		t.Run(fmt.Sprintf("%s/pool-%d", tc.mode, tc.pool), func(t *testing.T) {
			broken := tc.pool
			object := fmt.Sprintf("conflict-%s-%d", tc.mode, broken)
			base := UTCNow().Add(-time.Hour)
			putConsistencyObject(t, z, bucket, object, 1-broken, "old-acknowledged", ObjectOptions{MTime: base})
			set := z.serverPools[broken].getHashedSet(object)
			disks := set.getDisks()
			saved := make([][]byte, len(disks))
			for generation := range 4 {
				putConsistencyObject(t, z, bucket, object, broken, fmt.Sprintf("newer-%d", generation),
					ObjectOptions{MTime: base.Add(time.Duration(generation+1) * time.Minute)})
				for i, disk := range disks {
					g := i % 4
					switch tc.mode {
					case "older-quorum":
						g = 0
						if i >= len(disks)-3 {
							g = i - (len(disks) - 3) + 1
						}
					case "newer-quorum":
						g = 3
						if i < 3 {
							g = i
						}
					}
					if g != generation {
						continue
					}
					buf, err := disk.ReadAll(ctx, bucket, pathJoin(object, xlStorageFormatFile))
					if err != nil {
						t.Fatal(err)
					}
					saved[i] = bytes.Clone(buf)
				}
			}
			for i, disk := range disks {
				if err := disk.WriteAll(ctx, bucket, pathJoin(object, xlStorageFormatFile), saved[i]); err != nil {
					t.Fatal(err)
				}
			}
			for _, version := range []string{"", nullVersionID} {
				for _, method := range []string{http.MethodHead, http.MethodGet} {
					rec := consistencyRequest(t, router, method, bucket, object, version)
					want := http.StatusServiceUnavailable
					if tc.mode == "newer-quorum" {
						want = http.StatusOK
					}
					if rec.Code != want {
						t.Errorf("%s version=%q: want status=%d, got=%d body=%q", method, version, want, rec.Code, rec.Body.String())
					}
					if want == http.StatusOK && method == http.MethodGet && rec.Body.String() != "newer-3" {
						t.Errorf("latest quorum did not return its payload: %q", rec.Body.String())
					}
				}
			}
			for i, disk := range disks {
				buf, err := disk.ReadAll(ctx, bucket, pathJoin(object, xlStorageFormatFile))
				if err != nil || !bytes.Equal(buf, saved[i]) {
					t.Errorf("read destroyed conflicting generation on disk %d: %v", i, err)
				}
			}
		})
	}
}

func TestPoolsReadUnavailablePoolDoesNotExposeOldValue(t *testing.T) {
	z, bucket := consistencyPools(t)
	for broken := range 2 {
		t.Run(fmt.Sprintf("broken-pool-%d", broken), func(t *testing.T) {
			object := fmt.Sprintf("unavailable-%d", broken)
			putConsistencyObject(t, z, bucket, object, 1-broken, "old-acknowledged", ObjectOptions{})
			set := z.serverPools[broken].getHashedSet(object)
			disks, switchDisks := installListingDiskSwitch(set)
			defer switchDisks(disks)
			faulty := append([]StorageAPI(nil), disks...)
			for i := range faulty {
				faulty[i] = consistencyReadFaultDisk{StorageAPI: faulty[i], bucket: bucket, object: object}
			}
			switchDisks(faulty)
			if _, err := z.GetObjectInfo(t.Context(), bucket, object, ObjectOptions{}); err == nil || isErrObjectNotFound(err) {
				t.Errorf("unknown pool state allowed an old value: %v", err)
			}
		})
	}
}
