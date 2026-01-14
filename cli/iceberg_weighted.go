package cli

import (
	"github.com/minio/cli"
	"github.com/minio/pkg/v3/console"
	"github.com/minio/warp/pkg/bench"
	"github.com/minio/warp/pkg/iceberg"
	"github.com/minio/warp/pkg/iceberg/rest"
)

var icebergWeightedFlags = []cli.Flag{
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
		Value: "benchmark_catalog",
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
	cli.StringFlag{
		Name:  "base-location",
		Usage: "Base storage location for tables",
		Value: "s3://benchmark",
	},
	cli.Int64Flag{
		Name:  "seed",
		Usage: "Random seed for reproducibility",
		Value: 42,
	},
	cli.IntFlag{
		Name:  "readers",
		Usage: "Number of reader threads",
		Value: 8,
	},
	cli.Float64Flag{
		Name:  "reader-mean",
		Usage: "Mean position (0.0-1.0) for reader distribution",
		Value: 0.3,
	},
	cli.Float64Flag{
		Name:  "reader-variance",
		Usage: "Variance for reader distribution",
		Value: 0.0278,
	},
	cli.IntFlag{
		Name:  "writers",
		Usage: "Number of writer threads",
		Value: 2,
	},
	cli.Float64Flag{
		Name:  "writer-mean",
		Usage: "Mean position (0.0-1.0) for writer distribution",
		Value: 0.7,
	},
	cli.Float64Flag{
		Name:  "writer-variance",
		Usage: "Variance for writer distribution",
		Value: 0.0278,
	},
}

var icebergWeightedCombinedFlags = combineFlags(globalFlags, icebergWeightedFlags, benchFlags, analyzeFlags)

var icebergWeightedCmd = cli.Command{
	Name:   "iceberg-weighted",
	Usage:  "benchmark Iceberg REST catalog with weighted/skewed access patterns",
	Action: mainIcebergWeighted,
	Before: setGlobalsFromContext,
	Flags:  icebergWeightedCombinedFlags,
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS]

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}

EXAMPLES:
  1. Run weighted workload with default 80/20 style distribution:
     {{.HelpName}} --catalog-uri http://localhost:9001/_iceberg --iceberg-access-key minioadmin --iceberg-secret-key minioadmin

  2. Run with more readers focused on different tables than writers:
     {{.HelpName}} --readers 10 --reader-mean 0.2 --writers 2 --writer-mean 0.8 --catalog-uri http://localhost:9001/_iceberg

  3. High contention workload (readers and writers on same tables):
     {{.HelpName}} --reader-mean 0.5 --writer-mean 0.5 --reader-variance 0.01 --writer-variance 0.01
`,
}

func mainIcebergWeighted(ctx *cli.Context) error {
	checkIcebergWeightedSyntax(ctx)

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
		BaseLocation:   ctx.String("base-location"),
		CatalogName:    ctx.String("catalog-name"),
	}

	readers := []bench.WeightedDistribution{
		{
			Count:    ctx.Int("readers"),
			Mean:     ctx.Float64("reader-mean"),
			Variance: ctx.Float64("reader-variance"),
		},
	}

	writers := []bench.WeightedDistribution{
		{
			Count:    ctx.Int("writers"),
			Mean:     ctx.Float64("writer-mean"),
			Variance: ctx.Float64("writer-variance"),
		},
	}

	b := bench.IcebergWeighted{
		Common:     getIcebergCommon(ctx),
		RestClient: restClient,
		TreeConfig: treeCfg,
		Readers:    readers,
		Writers:    writers,
		Seed:       ctx.Int64("seed"),
	}

	return runBench(ctx, &b)
}

func checkIcebergWeightedSyntax(ctx *cli.Context) {
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
	if ctx.Int("readers") < 0 {
		console.Fatal("--readers must be >= 0")
	}
	if ctx.Int("writers") < 0 {
		console.Fatal("--writers must be >= 0")
	}
	if ctx.Int("readers") == 0 && ctx.Int("writers") == 0 {
		console.Fatal("at least one reader or writer is required")
	}
	mean := ctx.Float64("reader-mean")
	if mean < 0.0 || mean > 1.0 {
		console.Fatal("--reader-mean must be between 0.0 and 1.0")
	}
	mean = ctx.Float64("writer-mean")
	if mean < 0.0 || mean > 1.0 {
		console.Fatal("--writer-mean must be between 0.0 and 1.0")
	}
	checkAnalyze(ctx)
	checkBenchmark(ctx)
}
