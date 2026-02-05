/*
 * Warp (C) 2019-2020 MinIO, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cli

import (
	"errors"
	"fmt"
	"strconv"
	"time"
	"math/rand"
	"strings"
	"sync/atomic"

	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/minio-go/v7"
	"github.com/minio/pkg/v3/console"
	"github.com/minio/warp/pkg/bench"
)

var putFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "obj.size",
		Value: "10MiB",
		Usage: "Size of each generated object. Can be a number or 10KiB/MiB/GiB. All sizes are base 2 binary.",
	},
	cli.StringFlag{
		Name:   "part.size",
		Value:  "",
		Usage:  "Multipart part size. Can be a number or 10KiB/MiB/GiB. All sizes are base 2 binary.",
		Hidden: true,
	},
	cli.BoolFlag{
		Name:  "post",
		Usage: "Use PostObject for upload. Will force single part upload",
	},
	cli.StringFlag{
		Name:  "mtime-age",
		Value: "",
		Usage: "Set object mtime to N days ago. Use 'N' for fixed or 'N-M' for random range (e.g., '45' or '41-45'). Useful for ILM expiry testing.",
	},
	cli.IntFlag{
		Name:  "ilm-expiry",
		Value: 0,
		Usage: "Set ILM expiry rule on bucket to expire objects older than N days. Use with --mtime-age for testing.",
	},
}

var PutCombinedFlags = combineFlags(globalFlags, ioFlags, putFlags, genFlags, benchFlags, analyzeFlags)

// Put command.
var putCmd = cli.Command{
	Name:   "put",
	Usage:  "benchmark put objects",
	Action: mainPut,
	Before: setGlobalsFromContext,
	Flags:  PutCombinedFlags,
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS]
  -> see https://github.com/minio/warp#put

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

// mainPut is the entry point for cp command.
func mainPut(ctx *cli.Context) error {
	checkPutSyntax(ctx)
	b := bench.Put{
		Common:     getCommon(ctx, newGenSource(ctx, "obj.size")),
		PostObject: ctx.Bool("post"),
	}
	b.Common.MtimeFunc = parseMtimeAge(ctx)
	b.Common.ILMExpiryDays = ctx.Int("ilm-expiry")
	return runBench(ctx, &b)
}

// parseMtimeAge parses mtime-age flag and returns a function that generates mtime.
// Returns nil if flag is not set.
func parseMtimeAge(ctx *cli.Context) func() time.Time {
	mtimeAge := ctx.String("mtime-age")
	if mtimeAge == "" {
		return nil
	}

	if strings.Contains(mtimeAge, "-") {
		parts := strings.SplitN(mtimeAge, "-", 2)
		minDays, err1 := strconv.Atoi(parts[0])
		maxDays, err2 := strconv.Atoi(parts[1])
		if err1 != nil || err2 != nil || minDays > maxDays || minDays < 0 {
			console.Fatalf("--mtime-age range must be 'MIN-MAX' where MIN <= MAX (e.g., '41-45')")
		}
		return func() time.Time {
			days := minDays + rand.Intn(maxDays-minDays+1)
			return time.Now().AddDate(0, 0, -days)
		}
	}

	days, err := strconv.Atoi(mtimeAge)
	if err != nil || days < 0 {
		console.Fatalf("--mtime-age must be a positive number of days or a range like '41-45')")
	}
	fixedTime := time.Now().AddDate(0, 0, -days)
	return func() time.Time {
		return fixedTime
	}
}


const metadataChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890-_."

// putOpts retrieves put options from the context.
func putOpts(ctx *cli.Context) minio.PutObjectOptions {
	pSize, _ := toSize(ctx.String("part.size"))
	useMD5, checksumType := parseChecksum(ctx)
	options := minio.PutObjectOptions{
		ServerSideEncryption: newSSE(ctx),
		DisableMultipart:     ctx.Bool("disable-multipart"),
		DisableContentSha256: ctx.Bool("disable-sha256-payload"),
		SendContentMd5:       useMD5,
		Checksum:             checksumType,
		StorageClass:         ctx.String("storage-class"),
		PartSize:             pSize,
	}

	for _, flag := range []string{"add-metadata", "tag"} {
		values := make(map[string]string)

		for _, v := range ctx.StringSlice(flag) {
			idx := strings.Index(v, "=")
			if idx <= 0 {
				console.Fatalf("--%s takes `key=value` argument", flag)
			}
			key := v[:idx]
			value := v[idx+1:]
			if len(value) == 0 {
				console.Fatalf("--%s value can't be empty", flag)
			}
			var randN int
			if _, err := fmt.Sscanf(value, "rand:%d", &randN); err == nil {
				rng := rand.New(rand.NewSource(int64(rand.Uint64())))
				value = ""
				for i := 0; i < randN; i++ {
					value += string(metadataChars[rng.Int()%len(metadataChars)])
				}
			}
			values[key] = value
		}

		switch flag {
		case "add-metadata":
			options.UserMetadata = values
		case "tag":
			options.UserTags = values
		}
	}

	return options
}

func checkPutSyntax(ctx *cli.Context) {
	if ctx.NArg() > 0 {
		console.Fatal("Command takes no arguments")
	}

	checkAnalyze(ctx)
	checkBenchmark(ctx)
}

var useTrailingHeaders atomic.Bool

func parseChecksum(ctx *cli.Context) (useMD5 bool, ct minio.ChecksumType) {
	useMD5 = ctx.Bool("md5")
	if cs := ctx.String("checksum"); cs != "" {
		switch strings.ToUpper(cs) {
		case "CRC32":
			ct = minio.ChecksumCRC32
		case "CRC32C":
			ct = minio.ChecksumCRC32C
		case "CRC32-FO":
			ct = minio.ChecksumFullObjectCRC32
		case "CRC32C-FO":
			ct = minio.ChecksumFullObjectCRC32C
		case "SHA1":
			ct = minio.ChecksumSHA1
		case "SHA256":
			ct = minio.ChecksumSHA256
		case "CRC64N", "CRC64NVME":
			ct = minio.ChecksumCRC64NVME
		case "MD5":
			useMD5 = true
		default:
			err := fmt.Errorf("unknown checksum type: %s. Should be one of CRC64NVME, MD5, CRC32, CRC32C, CRC32-FO, CRC32C-FO, SHA1 or SHA256", cs)
			fatalIf(probe.NewError(err), "")
		}
		if ct.IsSet() {
			useTrailingHeaders.Store(true)
			if useMD5 {
				err := errors.New("cannot combine MD5 with checksum")
				fatalIf(probe.NewError(err), "")
			}
		}
	}
	return useMD5, ct
}
