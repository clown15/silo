// Copyright (c) 2026 Feng Ruohang
// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"sync"
)

// This is an undo capability, not a distributed fencing token. Any subsequent
// mutation of the same version revokes it, including a no-op metadata update.
const rollbackIDKey = ReservedMetadataPrefixLower + "rollback-id"

// Bound memory independently of object count. Include the physical drive so
// shards of one object are not serialized across drives. Multiple paths are
// locked in slot order, including collisions, to avoid lock inversion.
var storageMutationLocks [65536]sync.Mutex

func (s *xlStorage) lockMutations(paths ...string) func() {
	var slots [4]int
	n := 0
	for _, path := range paths {
		h := uint64(14695981039346656037)
		for _, part := range []string{s.drivePath, path} {
			for i := range part {
				h = (h ^ uint64(part[i])) * 1099511628211
			}
			h *= 1099511628211
		}
		slot := int(h % uint64(len(storageMutationLocks)))
		if !slices.Contains(slots[:n], slot) {
			slots[n] = slot
			n++
		}
	}
	slices.Sort(slots[:n])
	for _, slot := range slots[:n] {
		storageMutationLocks[slot].Lock()
	}
	return func() {
		for i := n - 1; i >= 0; i-- {
			storageMutationLocks[slots[i]].Unlock()
		}
	}
}

// Compare logical version state, excluding per-shard checksums/index, physical
// DataDir placement and properties of other versions in the same xl.meta.
// Different DataDir values remain readable for the existing rebalance recovery
// contract; rollback separately requires an exact DataDir match.
func sameFileInfoVersion(a, b FileInfo) bool {
	if a.VersionID != b.VersionID || a.Deleted != b.Deleted || a.XLV1 != b.XLV1 || a.Size != b.Size ||
		a.TransitionStatus != b.TransitionStatus || a.TransitionTier != b.TransitionTier ||
		a.TransitionedObjName != b.TransitionedObjName || a.TransitionVersionID != b.TransitionVersionID ||
		!sameVersionMetadata(a.Metadata, b.Metadata) || !bytes.Equal(a.Checksum, b.Checksum) || len(a.Parts) != len(b.Parts) {
		return false
	}
	if !a.Deleted && a.Size != 0 && !a.Erasure.Equal(b.Erasure) {
		return false
	}
	for i, p := range a.Parts {
		q := b.Parts[i]
		if p.Number != q.Number || p.Size != q.Size || p.ActualSize != q.ActualSize || p.ETag != q.ETag || !bytes.Equal(p.Index, q.Index) {
			return false
		}
	}
	return true
}

// The undo capability controls rollback ownership, not the logical contents.
// A partial no-op update can revoke it without changing a single public field.
func sameVersionMetadata(a, b map[string]string) bool {
	if a[rollbackIDKey] == b[rollbackIDKey] {
		return maps.Equal(a, b)
	}
	for k, v := range a {
		if k != rollbackIDKey {
			if other, ok := b[k]; !ok || other != v {
				return false
			}
		}
	}
	for k := range b {
		if k != rollbackIDKey {
			if _, ok := a[k]; !ok {
				return false
			}
		}
	}
	return true
}

// Legacy callers without an undo capability must match the persisted state,
// rather than transient coordinator-only fields in their original FileInfo.
func samePersistedVersion(current, expected FileInfo) bool {
	if current.Metadata[rollbackIDKey] != expected.Metadata[rollbackIDKey] {
		return false
	}
	var meta xlMetaV2
	if err := meta.AddVersion(expected); err != nil {
		return false
	}
	fi, err := meta.ToFileInfo(current.Volume, current.Name, expected.VersionID, true, true)
	return err == nil && sameFileInfoVersion(current, fi)
}

// Caller holds the object's mutation guard. Restore just the overwritten
// version, and persist that decision before removing any external shards.
func (s *xlStorage) undoWrite(ctx context.Context, volume, path string, fi FileInfo, opts DeleteOptions, meta *xlMetaV2) error {
	versionID := fi.VersionID
	if versionID == "" {
		versionID = nullVersionID
	}
	current, err := meta.ToFileInfo(volume, path, versionID, true, true)
	if err != nil {
		return err
	}
	if current.DataDir != fi.DataDir || !current.ModTime.Equal(fi.ModTime) || !samePersistedVersion(current, fi) {
		return errFileVersionNotFound
	}
	var previous xlMetaV2
	var oldVersion *xlMetaV2Version
	var oldData []byte
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return err
	}
	if opts.OldDataDir != "" {
		buf, err := s.readAllData(ctx, volume, volumeDir, pathJoin(volumeDir, path, opts.OldDataDir, xlStorageFormatFileBackup))
		if err != nil {
			return err
		}
		defer metaDataPoolPut(buf)
		if err := previous.LoadOrConvert(buf); err != nil {
			return err
		}
		_, oldVersion, err = previous.findVersionStr(versionID)
		if err != nil {
			return err
		}
		oldData = previous.data.find(versionID)
		if oldData == nil {
			oldData = previous.data.find(oldVersion.getDataDir())
		}
	}
	dataDir, err := meta.DeleteVersion(fi)
	if err != nil {
		return err
	}
	if dataDir != "" {
		meta.data.remove(versionID, dataDir)
	}
	if oldVersion != nil {
		if err := meta.addVersion(*oldVersion); err != nil {
			return err
		}
		if oldData != nil {
			meta.data.replace(versionID, oldData)
		}
		if dataDir == oldVersion.getDataDir() {
			dataDir = ""
		}
	}
	if len(meta.versions) > 0 {
		buf, err := meta.AppendTo(metaDataPoolGet())
		defer metaDataPoolPut(buf)
		if err != nil {
			return err
		}
		if err := s.writeAllMeta(ctx, volume, pathJoin(path, xlStorageFormatFile), buf, true); err != nil {
			return err
		}
	} else if err := s.deleteFile(volumeDir, pathJoin(volumeDir, path, xlStorageFormatFile), true, false); err != nil {
		return err
	}
	if dataDir != "" {
		if err := s.moveToTrash(pathJoin(volumeDir, path, dataDir), true, false); err != nil && !errors.Is(err, errFileNotFound) {
			return err
		}
	}
	if opts.OldDataDir != "" {
		backupDir := pathJoin(volumeDir, path, opts.OldDataDir)
		Remove(pathJoin(backupDir, xlStorageFormatFileBackup))
		if strings.HasSuffix(opts.OldDataDir, "-rollback") {
			Remove(backupDir)
		}
	}
	return nil
}
