// asmdev serves a web UI for looking into an ASM cluster. Enable WithEventTrace on the nodes to see their events.
//
//	go run ./devtool/cmd/asmdev -join 127.0.0.1:7101
//	go run ./devtool/cmd/asmdev -join 127.0.0.1:7101 -ca ca.pem -cert asmdev.pem -key asmdev-key.pem   # cluster with TLS
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"rumue.io/asm/devtool"
	asm "rumue.io/asm/v2"
)

func main() {
	join := flag.String("join", "127.0.0.1:7101", "address of a node of the cluster, the others are found through its peers")
	listen := flag.String("listen", "127.0.0.1:7700", "address of the web UI")
	poll := flag.Duration("poll", time.Second, "how often the nodes are inspected")
	limit := flag.Uint("limit", 500, "locks and containers shown per node, 0 shows all")
	caFile := flag.String("ca", "", "CA certificate of a cluster with TLS (PEM)")
	certFile := flag.String("cert", "", "certificate of the dev tool with the observer role (PEM)")
	keyFile := flag.String("key", "", "key of the certificate (PEM)")
	flag.Parse()

	var opts []devtool.Option
	// TLS needs all three files; some of them alone would silently dial without TLS
	if set := btoi(*caFile != "") + btoi(*certFile != "") + btoi(*keyFile != ""); set != 0 && set != 3 {
		log.Fatal("-ca, -cert and -key must be set together (cluster with TLS) or not at all")
	}
	if *caFile != "" {
		ca, cert, err := asm.LoadTLSFiles(*caFile, *certFile, *keyFile)
		if err != nil {
			log.Fatalf("loading the TLS files: %v", err)
		}
		opts = append(opts, devtool.WithCredentials(asm.ToolCredentials(ca, cert)))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	s := devtool.NewServer(*join, *poll, uint32(*limit), opts...)
	go s.Run(ctx)

	srv := &http.Server{Addr: *listen, Handler: s.Handler()}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()

	log.Printf("asm devtool for the cluster of %s on http://%s", *join, *listen)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}
