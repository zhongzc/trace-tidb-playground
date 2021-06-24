package main

import (
    "context"
    "database/sql"
    "fmt"
    "io"
    "log"

    _ "github.com/go-sql-driver/mysql"
    "github.com/opentracing/opentracing-go"
    "github.com/uber/jaeger-client-go"
    jaegercfg "github.com/uber/jaeger-client-go/config"
)

func main() {
    db := initDBClient()
    defer db.Close()

    closer := initTracer()
    defer closer.Close()

    (func() {
        ctx := context.Background()

        span := opentracing.GlobalTracer().StartSpan("Trace TiDB Playground")
        defer span.Finish()

        conn, err := db.Conn(ctx)
        if err != nil {
            log.Fatal(err)
        }
        defer conn.Close()

        (func() {
            span := span.Tracer().StartSpan("create a table", opentracing.ChildOf(span.Context()))
            defer span.Finish()
            mustExec(ctx, conn, fmt.Sprintf("SET tidb_trace_id='%s'", span.Context().(jaeger.SpanContext)))
            mustExec(ctx, conn, "DROP TABLE IF EXISTS t_101;")
            mustExec(ctx, conn, "CREATE TABLE t_101 (a INT AUTO_INCREMENT, b INT, c VARCHAR(100), PRIMARY KEY (a));")
        })()

        (func() {
            span := span.Tracer().StartSpan("insert rows", opentracing.ChildOf(span.Context()))
            defer span.Finish()
            mustExec(ctx, conn, fmt.Sprintf("SET tidb_trace_id='%s'", span.Context().(jaeger.SpanContext)))
            mustExec(ctx, conn, "INSERT INTO t_101 (b, c) VALUES (1, '1'),(1, '1'),(1, '1'),(1, '1'),(1, '1');")
            mustExec(ctx, conn, "INSERT INTO t_101 SELECT NULL as a, b, c FROM t_101;")
            mustExec(ctx, conn, "INSERT INTO t_101 SELECT NULL as a, b, c FROM t_101;")
            mustExec(ctx, conn, "INSERT INTO t_101 SELECT NULL as a, b, c FROM t_101;")
        })()

        (func() {
            span := span.Tracer().StartSpan("query", opentracing.ChildOf(span.Context()))
            defer span.Finish()
            mustExec(ctx, conn, fmt.Sprintf("SET tidb_trace_id='%s'", span.Context().(jaeger.SpanContext)))
            mustQuery(ctx, conn, "SELECT sum(b) FROM t_101;")
        })()
    })()
}

func initDBClient() *sql.DB {
    db, err := sql.Open("mysql", "root@tcp(localhost:4000)/test")
    if err != nil {
        log.Fatal(err)
    }
    return db
}

func initTracer() io.Closer {
    cfg := jaegercfg.Configuration{
        ServiceName: "trace-tidb-playground",
        Sampler: &jaegercfg.SamplerConfig{
            Type:  jaeger.SamplerTypeConst,
            Param: 1,
        },
        Reporter: &jaegercfg.ReporterConfig{},
    }

    tracer, closer, err := cfg.NewTracer()
    if err != nil {
        log.Fatal(err)
    }
    opentracing.SetGlobalTracer(tracer)

    return closer
}

func mustExec(ctx context.Context, db *sql.Conn, sql string) {
    _, err := db.ExecContext(ctx, sql)
    if err != nil {
        log.Fatal(err)
    }
}

func mustQuery(ctx context.Context, db *sql.Conn, sql string) {
    rows, err := db.QueryContext(ctx, sql)
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
    cols, err := rows.Columns()
    if err != nil {
        log.Fatal(err)
    }
    for rows.Next() {
        columns := make([]string, len(cols))
        columnPointers := make([]interface{}, len(cols))
        for i, _ := range columns {
            columnPointers[i] = &columns[i]
        }
        err := rows.Scan(columnPointers...)
        if err != nil {
            log.Fatal(err)
        }
    }
}
