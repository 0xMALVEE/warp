package cli

import (
	"github.com/minio/cli"
	"github.com/minio/pkg/v3/console"
	"github.com/minio/warp/pkg/bench"
	"github.com/minio/warp/pkg/iceberg"
	"github.com/minio/warp/pkg/iceberg/rest"
)

var icebergReadFlags = []cli.Flag{
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
}

var icebergReadCombinedFlags = combineFlags(globalFlags, icebergReadFlags, benchFlags, analyzeFlags)

var icebergReadCmd = cli.Command{
	Name:   "iceberg-read",
	Usage:  "benchmark Iceberg REST catalog read operations (creates dataset in prepare, then benchmarks reads)",
	Action: mainIcebergRead,
	Before: setGlobalsFromContext,
	Flags:  icebergReadCombinedFlags,
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS]

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}

EXAMPLES:
  1. Run Iceberg read benchmark against MinIO S3 Tables:
     {{.HelpName}} --catalog-uri http://localhost:9001/_iceberg --iceberg-access-key minioadmin --iceberg-secret-key minioadmin

  2. Create a larger dataset with more tables:
     {{.HelpName}} --namespace-width 3 --namespace-depth 4 --tables-per-ns 10 --catalog-uri http://localhost:9001/_iceberg
`,
}

func mainIcebergRead(ctx *cli.Context) error {
	checkIcebergReadSyntax(ctx)

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

	b := bench.IcebergRead{
		Common:     getIcebergCommon(ctx),
		RestClient: restClient,
		TreeConfig: treeCfg,
	}

	return runBench(ctx, &b)
}

func checkIcebergReadSyntax(ctx *cli.Context) {
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
	checkAnalyze(ctx)
	checkBenchmark(ctx)
}

func getIcebergCommon(ctx *cli.Context) bench.Common {
	statusln := func(s string) {
		console.Eraseline()
		console.Print(s)
	}
	if globalQuiet {
		statusln = func(_ string) {}
	}

	return bench.Common{
		Concurrency:  ctx.Int("concurrent"),
		UpdateStatus: statusln,
		TotalClients: 1,
		Error: func(data ...any) {
			console.Errorln(data...)
		},
	}
}
