package sectorstorage

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/xerrors"

	ffi "github.com/filecoin-project/filecoin-ffi"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-actors/actors/runtime/proof"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

// FaultTracker TODO: Track things more actively
type FaultTracker interface {
	CheckProvable(ctx context.Context, pp abi.RegisteredPoStProof, sectors []storage.SectorRef, rg storiface.RGetter) (map[abi.SectorID]string, error)
}

// CheckProvable returns unprovable sectors
func (m *Manager) CheckProvable(ctx context.Context, pp abi.RegisteredPoStProof, sectors []storage.SectorRef, rg storiface.RGetter) (map[abi.SectorID]string, error) {
	var bad = make(map[abi.SectorID]string)

	ssize, err := pp.SectorSize()
	if err != nil {
		return nil, err
	}

	// TODO: More better checks
	for _, sector := range sectors {
		err := func() error {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			locked, err := m.index.StorageTryLock(ctx, sector.ID, storiface.FTSealed|storiface.FTCache, storiface.FTNone)
			if err != nil {
				return xerrors.Errorf("acquiring sector lock: %w", err)
			}

			if !locked {
				log.Warnw("CheckProvable Sector FAULT: can't acquire read lock", "sector", sector)
				bad[sector.ID] = fmt.Sprint("can't acquire read lock")
				return nil
			}

			lp, _, err := m.localStore.AcquireSector(ctx, sector, storiface.FTSealed|storiface.FTCache, storiface.FTNone, storiface.PathStorage, storiface.AcquireMove)
			if err != nil {
				log.Warnw("CheckProvable Sector FAULT: acquire sector in checkProvable", "sector", sector, "error", err)
				bad[sector.ID] = fmt.Sprintf("acquire sector failed: %s", err)
				return nil
			}

			sealedPath := storiface.PathByType(lp, storiface.FTSealed)
			cachePath := storiface.PathByType(lp, storiface.FTCache)
			sealedSectorPath := storiface.PathExtByType(lp, storiface.FTSealed)
			cacheSectorPath := storiface.PathExtByType(lp, storiface.FTCache)

			if sealedPath == "" || cachePath == "" {
				log.Warnw("CheckProvable Sector FAULT: cache and/or sealed paths not found", "sector", sector, "sealed", sealedPath, "cache", cachePath)
				bad[sector.ID] = fmt.Sprintf("cache and/or sealed paths not found, cache %q, sealed %q", cachePath, sealedPath)
				return nil
			}

			localToCheck := map[string]int64{}

			type ossCheck struct {
				sectorPath  storiface.SectorPath
				sectorFiles []string
				checkSize   bool
				fileType    storiface.SectorFileType
			}
			ossToCheck := []ossCheck{}

			sealedSectorPathInfo := ffi.PrivateSectorPathInfo{}

			if sealedSectorPath.Oss {
				ossToCheck = append(ossToCheck, ossCheck{
					sectorPath: sealedSectorPath,
					checkSize:  true,
					fileType:   storiface.FTSealed,
				})
				sealedSectorPathInfo.Url = sealedSectorPath.OssInfo.URL
				sealedSectorPathInfo.AccessKey = sealedSectorPath.OssInfo.AccessKey
				sealedSectorPathInfo.SecretKey = sealedSectorPath.OssInfo.SecretKey
				sealedSectorPathInfo.LandedDir = sealedSectorPath.OssInfo.LandedDir
				sealedSectorPathInfo.BucketName = sealedSectorPath.OssInfo.BucketName
				sealedSectorPathInfo.SectorName = sealedSectorPath.OssInfo.SectorName
			} else {
				localToCheck[sealedPath] = 1
			}

			cacheSectorPathInfo := ffi.PrivateSectorPathInfo{}

			if cacheSectorPath.Oss {
				ossToCheck = append(ossToCheck, ossCheck{
					sectorPath:  cacheSectorPath,
					sectorFiles: getCacheFilesForSectorSize(cachePath, ssize),
					checkSize:   false,
					fileType:    storiface.FTCache,
				})
				cacheSectorPathInfo.Url = cacheSectorPath.OssInfo.URL
				cacheSectorPathInfo.AccessKey = cacheSectorPath.OssInfo.AccessKey
				cacheSectorPathInfo.SecretKey = cacheSectorPath.OssInfo.SecretKey
				cacheSectorPathInfo.LandedDir = cacheSectorPath.OssInfo.LandedDir
				cacheSectorPathInfo.BucketName = cacheSectorPath.OssInfo.BucketName
				cacheSectorPathInfo.SectorName = cacheSectorPath.OssInfo.SectorName
			} else {
				localToCheck[filepath.Join(cachePath, "t_aux")] = 0
				localToCheck[filepath.Join(cachePath, "p_aux")] = 0
				addCachePathsForSectorSize(localToCheck, cachePath, ssize)
			}

			for p, sz := range localToCheck {
				st, err := os.Stat(p)
				if err != nil {
					log.Warnw("CheckProvable Sector FAULT: sector file stat error", "sector", sector, "sealed", lp.Sealed, "cache", lp.Cache, "file", p, "err", err)
					bad[sector.ID] = fmt.Sprintf("%s", err)
					return nil
				}

				if sz != 0 {
					if st.Size() != int64(ssize)*sz {
						log.Warnw("CheckProvable Sector FAULT: sector file is wrong size", "sector", sector, "sealed", lp.Sealed, "cache", lp.Cache, "file", p, "size", st.Size(), "expectSize", int64(ssize)*sz)
						bad[sector.ID] = fmt.Sprintf("%s is wrong size (got %d, expect %d)", p, st.Size(), int64(ssize)*sz)
						return nil
					}
				}
			}

			for _, check := range ossToCheck {
				if err = m.localStore.CheckSectorInOss(ctx, check.sectorPath, check.sectorFiles, check.fileType, ssize, check.checkSize); err != nil {
					log.Warnw("CheckProvable Sector FAULT: oss check error", "sector", sector, "sealed", sealedPath, "cache", cachePath, "files", check.sectorFiles)
					bad[sector.ID] = fmt.Sprintf("%s is wrong in oss (%v)", check.sectorPath.OssInfo.SectorName, err)
					return nil
				}
			}

			if rg != nil {
				wpp, err := sector.ProofType.RegisteredWindowPoStProof()
				if err != nil {
					return err
				}

				var pr abi.PoStRandomness = make([]byte, abi.RandomnessLength)
				_, _ = rand.Read(pr)
				pr[31] &= 0x3f

				ch, err := ffi.GeneratePoStFallbackSectorChallenges(wpp, sector.ID.Miner, pr, []abi.SectorNumber{
					sector.ID.Number,
				})
				if err != nil {
					log.Warnw("CheckProvable Sector FAULT: generating challenges", "sector", sector, "sealed", lp.Sealed, "cache", lp.Cache, "err", err)
					bad[sector.ID] = fmt.Sprintf("generating fallback challenges: %s", err)
					return nil
				}

				commr, err := rg(ctx, sector.ID)
				if err != nil {
					log.Warnw("CheckProvable Sector FAULT: getting commR", "sector", sector, "sealed", lp.Sealed, "cache", lp.Cache, "err", err)
					bad[sector.ID] = fmt.Sprintf("getting commR: %s", err)
					return nil
				}

				_, err = ffi.GenerateSingleVanillaProof(ffi.PrivateSectorInfo{
					SectorInfo: proof.SectorInfo{
						SealProof:    sector.ProofType,
						SectorNumber: sector.ID.Number,
						SealedCID:    commr,
					},
					CacheDirPath:         storiface.PathByType(lp, storiface.FTCache),
					CacheInOss:           cacheSectorPath.Oss,
					CacheSectorPathInfo:  cacheSectorPathInfo,
					PoStProofType:        wpp,
					SealedSectorPath:     storiface.PathByType(lp, storiface.FTSealed),
					SealedInOss:          sealedSectorPath.Oss,
					SealedSectorPathInfo: sealedSectorPathInfo,
				}, ch.Challenges[sector.ID.Number])
				if err != nil {
					log.Warnw("CheckProvable Sector FAULT: generating vanilla proof", "sector", sector, "sealed", lp.Sealed, "cache", lp.Cache, "err", err)
					bad[sector.ID] = fmt.Sprintf("generating vanilla proof: %s", err)
					return nil
				}
			}
			return nil
		}()
		if err != nil {
			return nil, err
		}
	}

	return bad, nil
}

func addCachePathsForSectorSize(chk map[string]int64, cacheDir string, ssize abi.SectorSize) {
	switch ssize {
	case 2 << 10:
		fallthrough
	case 8 << 20:
		fallthrough
	case 512 << 20:
		chk[filepath.Join(cacheDir, "sc-02-data-tree-r-last.dat")] = 0
	case 32 << 30:
		for i := 0; i < 8; i++ {
			chk[filepath.Join(cacheDir, fmt.Sprintf("sc-02-data-tree-r-last-%d.dat", i))] = 0
		}
	case 64 << 30:
		for i := 0; i < 16; i++ {
			chk[filepath.Join(cacheDir, fmt.Sprintf("sc-02-data-tree-r-last-%d.dat", i))] = 0
		}
	default:
		log.Warnf("not checking cache files of %s sectors for faults", ssize)
	}
}

func getCacheFilesForSectorSize(cacheDir string, ssize abi.SectorSize) []string {
	files := []string{}

	switch ssize {
	case 2 << 10:
		fallthrough
	case 8 << 20:
		fallthrough
	case 512 << 20:
		files = append(files, filepath.Join(cacheDir, "sc-02-data-tree-r-last.dat"))
	case 32 << 30:
		for i := 0; i < 8; i++ {
			files = append(files, filepath.Join(cacheDir, fmt.Sprintf("sc-02-data-tree-r-last-%d.dat", i)))
		}
	case 64 << 30:
		for i := 0; i < 16; i++ {
			files = append(files, filepath.Join(cacheDir, fmt.Sprintf("sc-02-data-tree-r-last-%d.dat", i)))
		}
	default:
		log.Warnf("not checking cache files of %s sectors for faults", ssize)
	}
	return files
}

var _ FaultTracker = &Manager{}
