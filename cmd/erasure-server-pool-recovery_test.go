// Copyright (c) 2026 Feng Ruohang
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// An acknowledged recovery overwrite must resolve the unreadable pool, even
// when another pool already has a readable copy of the same object.
// Real inline metadata is combined in the same 15-old/1-new state used by
// that regression. Capacity adapters only make destination choice deterministic.
func TestPoolsMinorityOverwriteRecovery(t *testing.T) {
	for _, mode := range []string{"single-pool", "dual-pool-only-inconsistent-copy", "dual-pool-readable-other-copy"} {
		t.Run(mode, func(t *testing.T) {
			var z *erasureServerPools
			if mode == "single-pool" {
				z, _ = mutationPools(t)
			} else {
				z, _ = consistencyPools(t)
			}
			const object = "review-retry"
			for _, pool := range z.serverPools {
				restore := conditionalPutSwapDisks(pool, object, func(d StorageAPI) StorageAPI { return conditionalPutCapacityDisk{StorageAPI: d} })
				defer restore()
			}
			bucket, router := conditionalPutBucket(t, z, "unversioned")
			oldTime := time.Now().UTC().Add(-time.Hour)
			oldBody := bytes.Repeat([]byte{'a'}, 8192)
			pendingBody := bytes.Repeat([]byte{'b'}, 8192)
			recoveryBody := bytes.Repeat([]byte{'c'}, 8192)
			seed := func(pool int, body []byte, when time.Time) {
				t.Helper()
				putConsistencyObject(t, z, bucket, object, pool, string(body), ObjectOptions{MTime: when})
			}
			seed(0, oldBody, oldTime)
			disks := z.serverPools[0].getHashedSet(object).getDisks()
			oldMeta := make([][]byte, len(disks))
			for i, disk := range disks {
				oldMeta[i] = mustReadNullQuorumMeta(t, disk, bucket, object)
			}
			seed(0, pendingBody, oldTime.Add(time.Minute))
			for i := 0; i < len(disks)-1; i++ {
				if err := disks[i].WriteAll(t.Context(), bucket, object+"/"+xlStorageFormatFile, oldMeta[i]); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "dual-pool-readable-other-copy" {
				seed(1, oldBody, oldTime.Add(-time.Minute))
				// Ordinary allocation can select this healthy other pool.
				// Force that permitted choice without altering metadata reads.
				restore := conditionalPutPool(t, z, object, 1)
				defer restore()
			}
			before := consistencyRequest(t, router, http.MethodGet, bucket, object, "")
			if before.Code != http.StatusServiceUnavailable {
				t.Fatalf("fixture must refuse ambiguous read; got %d", before.Code)
			}
			req, err := newTestSignedRequestV4(http.MethodPut, getGetObjectURL("", bucket, object), int64(len(recoveryBody)), bytes.NewReader(recoveryBody), globalActiveCred.AccessKey, globalActiveCred.SecretKey, nil)
			if err != nil {
				t.Fatal(err)
			}
			result := httptest.NewRecorder()
			router.ServeHTTP(result, req.WithContext(t.Context()))
			t.Logf("recovery PUT HTTP=%d response=%s", result.Code, result.Body.String())
			if result.Code != http.StatusOK {
				t.Fatalf("unconditional recovery PUT: %d %s", result.Code, result.Body.String())
			}
			after := consistencyRequest(t, router, http.MethodGet, bucket, object, "")
			t.Logf("after acknowledged recovery PUT: GET HTTP=%d matches_new=%t", after.Code, bytes.Equal(after.Body.Bytes(), recoveryBody))
			for i, pool := range z.serverPools {
				oi, err := pool.GetObjectInfo(t.Context(), bucket, object, ObjectOptions{})
				t.Logf("pool=%d mtime=%s err=%v", i, oi.ModTime, err)
			}
			if after.Code != http.StatusOK || !bytes.Equal(after.Body.Bytes(), recoveryBody) {
				t.Fatalf("acknowledged recovery overwrite did not restore reads: HTTP=%d body=%s", after.Code, after.Body.String())
			}
		})
	}
}
