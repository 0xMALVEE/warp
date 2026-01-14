package cli

import (
	"github.com/minio/cli"
	"github.com/minio/pkg/v3/console"
	"github.com/minio/warp/pkg/bench"
	"github.com/minio/warp/pkg/iceberg"
	"github.com/minio/warp/pkg/iceberg/rest"
)

var icebergMixedFlags = []cli.Flag{
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
	cli.IntFlag{
		Name:  "columns",
		Usage: "Number of columns per table/view schema",
		Value: 10,
	},
	cli.IntFlag{
		Name:  "properties",
		Usage: "Number of properties per entity",
		Value: 5,
	},
	cli.StringFlag{
		Name:  "base-location",
		Usage: "Base storage location for tables",
		Value: "s3://benchmark",
	},
	cli.Float64Flag{
		Name:  "read-ratio",
		Usage: "Ratio of read operations (0.0-1.0). Rest are updates.",
		Value: 0.5,
	},
}

var icebergMixedCombinedFlags = combineFlags(globalFlags, icebergMixedFlags, benchFlags, analyzeFlags)

var icebergMixedCmd = cli.Command{
	Name:   "iceberg-mixed",
	Usage:  "benchmark mixed read/update workload on existing Iceberg REST catalog dataset",
	Action: mainIcebergMixed,
	Before: setGlobalsFromContext,
	Flags:  icebergMixedCombinedFlags,
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS]

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}

EXAMPLES:
  1. Run mixed read/update benchmark with 50% reads:
     {{.HelpName}} --catalog-uri http://localhost:9001/_iceberg --iceberg-access-key minioadmin --iceberg-secret-key minioadmin

  2. Run read-heavy workload (80% reads):
     {{.HelpName}} --read-ratio 0.8 --catalog-uri http://localhost:9001/_iceberg

  3. Run write-heavy workload (20% reads):
     {{.HelpName}} --read-ratio 0.2 --catalog-uri http://localhost:9001/_iceberg
`,
}

func mainIcebergMixed(ctx *cli.Context) error {
	checkIcebergMixedSyntax(ctx)

	restClient := rest.NewClient(rest.ClientConfig{
		BaseURL:   ctx.String("catalog-uri"),
		APIPrefix: ctx.String("api-prefix"),
		AccessKey: ctx.String("iceberg-access-key"),
		SecretKey: ctx.String("iceberg-secret-key"),
		Region:    ctx.String("iceberg-region"),
		Service:   ctx.String("iceberg-service"),
	})

	treeCfg := iceberg.TreeConfig{
		NamespaceWidth:   ctx.Int("namespace-width"),
		NamespaceDepth:   ctx.Int("namespace-depth"),
		TablesPerNS:      ctx.Int("tables-per-ns"),
		ViewsPerNS:       ctx.Int("views-per-ns"),
		ColumnsPerTable:  ctx.Int("columns"),
		ColumnsPerView:   ctx.Int("columns"),
		PropertiesPerNS:  ctx.Int("properties"),
		PropertiesPerTbl: ctx.Int("properties"),
		PropertiesPerVw:  ctx.Int("properties"),
		BaseLocation:     ctx.String("base-location"),
		CatalogName:      ctx.String("catalog-name"),
	}

	b := bench.IcebergMixed{
		Common:     getIcebergCommon(ctx),
		RestClient: restClient,
		TreeConfig: treeCfg,
		ReadRatio:  ctx.Float64("read-ratio"),
	}

	return runBench(ctx, &b)
}

func checkIcebergMixedSyntax(ctx *cli.Context) {
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
	if ctx.Int("namespace-width") < 1 {
		console.Fatal("--namespace-width must be at least 1")
	}
	if ctx.Int("namespace-depth") < 1 {
		console.Fatal("--namespace-depth must be at least 1")
	}
	readRatio := ctx.Float64("read-ratio")
	if readRatio < 0.0 || readRatio > 1.0 {
		console.Fatal("--read-ratio must be between 0.0 and 1.0")
	}
	checkAnalyze(ctx)
	checkBenchmark(ctx)
}
