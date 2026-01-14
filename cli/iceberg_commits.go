package cli

import (
	"github.com/minio/cli"
	"github.com/minio/pkg/v3/console"
	"github.com/minio/warp/pkg/bench"
	"github.com/minio/warp/pkg/iceberg"
	"github.com/minio/warp/pkg/iceberg/rest"
)

var icebergCommitsFlags = []cli.Flag{
	cli.IntFlag{
		Name:  "concurrent",
		Value: 20,
		Usage: "Run this many concurrent operations",
	},
	cli.StringFlag{
		Name:   "catalog-uri",
		Usage:  "Iceberg REST catalog base URL (e.g., http://localhost:9001/_iceberg)",
		EnvVar: "WARP_ICEBERG_CATALOG_URI",
		Value:  "http://127.0.0.1:9001/_iceberg",
	},
	cli.StringFlag{
		Name:   "api-prefix",
		Usage:  "API prefix for Iceberg REST catalog",
		EnvVar: "WARP_ICEBERG_API_PREFIX",
		Value:  "/v1",
	},
	cli.StringFlag{
		Name:   "iceberg-access-key",
		Usage:  "Access key for SIGV4 authentication",
		EnvVar: "WARP_ICEBERG_ACCESS_KEY",
		Value:  "",
	},
	cli.StringFlag{
		Name:   "iceberg-secret-key",
		Usage:  "Secret key for SIGV4 authentication",
		EnvVar: "WARP_ICEBERG_SECRET_KEY",
		Value:  "",
	},
	cli.StringFlag{
		Name:   "iceberg-region",
		Usage:  "Region for SIGV4 signing",
		EnvVar: "WARP_ICEBERG_REGION",
		Value:  "us-east-1",
	},
	cli.StringFlag{
		Name:   "iceberg-service",
		Usage:  "Service name for SIGV4 signing",
		EnvVar: "WARP_ICEBERG_SERVICE",
		Value:  "s3tables",
	},
	cli.StringFlag{
		Name:  "catalog-name",
		Usage: "Catalog name to use",
		Value: "benchmarkcatalog",
	},
	cli.IntFlag{
		Name:  "namespace-width",
		Usage: "Width of the N-ary namespace tree (children per namespace)",
		Value: 2,
	},
	cli.IntFlag{
		Name:  "namespace-depth",
		Usage: "Depth of the N-ary namespace tree",
		Value: 3,
	},
	cli.IntFlag{
		Name:  "tables-per-ns",
		Usage: "Number of tables per leaf namespace",
		Value: 5,
	},
	cli.IntFlag{
		Name:  "views-per-ns",
		Usage: "Number of views per leaf namespace",
		Value: 0,
	},
	cli.StringFlag{
		Name:  "base-location",
		Usage: "Base storage location for tables",
		Value: "s3://benchmark",
	},
	cli.IntFlag{
		Name:  "table-commits-throughput",
		Usage: "Number of concurrent table commit workers (0 = half of --concurrent)",
		Value: 0,
	},
	cli.IntFlag{
		Name:  "view-commits-throughput",
		Usage: "Number of concurrent view commit workers (0 = half of --concurrent)",
		Value: 0,
	},
}

var icebergCommitsCombinedFlags = combineFlags(globalFlags, icebergCommitsFlags, benchFlags, analyzeFlags)

var icebergCommitsCmd = cli.Command{
	Name:   "iceberg-commits",
	Usage:  "benchmark Iceberg REST catalog commit generation (updates table/view properties to create commits)",
	Action: mainIcebergCommits,
	Before: setGlobalsFromContext,
	Flags:  icebergCommitsCombinedFlags,
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS]

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}

EXAMPLES:
  1. Generate commits on existing dataset:
     {{.HelpName}} --catalog-uri http://localhost:9001/_iceberg --iceberg-access-key minioadmin --iceberg-secret-key minioadmin

  2. Generate commits with specific throughput:
     {{.HelpName}} --table-commits-throughput 10 --view-commits-throughput 5 --catalog-uri http://localhost:9001/_iceberg
`,
}

func mainIcebergCommits(ctx *cli.Context) error {
	checkIcebergCommitsSyntax(ctx)

	restClient := rest.NewClient(rest.ClientConfig{
		BaseURL:   ctx.String("catalog-uri"),
		APIPrefix: ctx.String("api-prefix"),
		AccessKey: ctx.String("iceberg-access-key"),
		SecretKey: ctx.String("iceberg-secret-key"),
		Region:    ctx.String("iceberg-region"),
		Service:   ctx.String("iceberg-service"),
	})

	treeCfg := iceberg.TreeConfig{
		NamespaceWidth: ctx.Int("namespace-width"),
		NamespaceDepth: ctx.Int("namespace-depth"),
		TablesPerNS:    ctx.Int("tables-per-ns"),
		ViewsPerNS:     ctx.Int("views-per-ns"),
		BaseLocation:   ctx.String("base-location"),
		CatalogName:    ctx.String("catalog-name"),
	}

	b := bench.IcebergCommits{
		Common:                 getIcebergCommon(ctx),
		RestClient:             restClient,
		TreeConfig:             treeCfg,
		TableCommitsThroughput: ctx.Int("table-commits-throughput"),
		ViewCommitsThroughput:  ctx.Int("view-commits-throughput"),
	}

	return runBench(ctx, &b)
}

func checkIcebergCommitsSyntax(ctx *cli.Context) {
	if ctx.NArg() > 0 {
		console.Fatal("Command takes no arguments")
	}
	if ctx.String("catalog-uri") == "" {
		console.Fatal("--catalog-uri is required")
	}
	if ctx.String("iceberg-access-key") == "" {
		console.Fatal("--iceberg-access-key is required")
	}
	if ctx.String("iceberg-secret-key") == "" {
		console.Fatal("--iceberg-secret-key is required")
	}
	checkAnalyze(ctx)
	checkBenchmark(ctx)
}
