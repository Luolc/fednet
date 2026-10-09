// Command fednet bridges Slack threads and the coding agents on each machine.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Luolc/fednet/internal/auth"
	"github.com/Luolc/fednet/internal/link"
	"github.com/Luolc/fednet/internal/store"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

const usage = `usage: fednet <command> [flags]

commands:
  hub -listen ADDR -db PATH
        run the hub
  hub register -db PATH CLIENT-ID HASH
        let a client connect; HASH is what its client init printed
  hub revoke -db PATH CLIENT-ID
        retire a client: its credential stops working
  client -hub URL -db PATH -credential PATH
        run a client on an agent machine
  client init -id CLIENT-ID -credential PATH
        create this machine's credential; prints CLIENT-ID and HASH, never the credential
  version
        print the version
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

// run is main without the process: it returns the exit code. 2 is a usage
// error, 1 any other failure.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("fednet", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, usage) }
	if err := fs.Parse(args); err != nil {
		return 2
	}
	var cmd func(context.Context, []string, io.Writer) error
	switch fs.Arg(0) {
	case "hub":
		cmd = hubCommand
	case "client":
		cmd = clientCommand
	case "version":
		fmt.Fprintln(stdout, version)
		return 0
	default:
		fs.Usage()
		return 2
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(stderr, nil)))
	err := cmd(ctx, fs.Args()[1:], stdout)
	var uerr usageError
	switch {
	case err == nil:
		return 0
	case errors.As(err, &uerr):
		fmt.Fprintf(stderr, "fednet: %v\n%s", err, usage)
		return 2
	default:
		fmt.Fprintf(stderr, "fednet: %v\n", err)
		return 1
	}
}

// usageError is a command-line mistake; run prints the usage after it.
type usageError string

func (e usageError) Error() string { return string(e) }

// parseFlags parses args with fs and checks for exactly want positional
// arguments. A flag error is already reported by fs.
func parseFlags(fs *flag.FlagSet, args []string, want int) error {
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return usageError(fs.Name() + ": " + err.Error())
	}
	if fs.NArg() != want {
		return usageError(fmt.Sprintf("%s: want %d arguments, got %d", fs.Name(), want, fs.NArg()))
	}
	return nil
}

func hubCommand(ctx context.Context, args []string, stdout io.Writer) error {
	if len(args) > 0 {
		switch args[0] {
		case "register":
			return hubRegister(ctx, args[1:])
		case "revoke":
			return hubRevoke(ctx, args[1:])
		}
	}
	return hubServe(ctx, args, stdout)
}

// hubServe runs the hub until ctx is done. It prints the address it listens
// on, so that -listen with port 0 is usable.
func hubServe(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("fednet hub", flag.ContinueOnError)
	listen := fs.String("listen", "", "address to listen on, such as 127.0.0.1:8080 (required)")
	dbPath := fs.String("db", "", "hub database file (required)")
	if err := parseFlags(fs, args, 0); err != nil {
		return err
	}
	if *listen == "" || *dbPath == "" {
		return usageError("fednet hub: -listen and -db are required")
	}
	st, err := store.OpenHub(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	hub := &link.Hub{Store: st, Identify: (&auth.Authenticator{Store: st}).Identify}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "listening on %s\n", ln.Addr())
	srv := &http.Server{Handler: hub.Handler(), ReadHeaderTimeout: 10 * time.Second}
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()
	select {
	case err := <-served:
		return err
	case <-ctx.Done():
	}
	// The downlink connections are hijacked WebSockets, which Shutdown
	// does not know about; the hub closes them itself.
	hub.Close()
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	err = srv.Shutdown(sctx)
	<-served
	return err
}

func hubRegister(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("fednet hub register", flag.ContinueOnError)
	dbPath := fs.String("db", "", "hub database file (required)")
	if err := parseFlags(fs, args, 2); err != nil {
		return err
	}
	if *dbPath == "" {
		return usageError("fednet hub register: -db is required")
	}
	id, hash := fs.Arg(0), fs.Arg(1)
	h, err := hex.DecodeString(hash)
	if err != nil || len(h) != sha256.Size {
		return usageError("fednet hub register: HASH must be the hex SHA-256 printed by client init")
	}
	st, err := store.OpenHub(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	return st.Register(ctx, id, h)
}

func hubRevoke(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("fednet hub revoke", flag.ContinueOnError)
	dbPath := fs.String("db", "", "hub database file (required)")
	if err := parseFlags(fs, args, 1); err != nil {
		return err
	}
	if *dbPath == "" {
		return usageError("fednet hub revoke: -db is required")
	}
	st, err := store.OpenHub(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.Revoke(ctx, fs.Arg(0)); errors.Is(err, store.ErrNotFound) {
		return fmt.Errorf("client %q is not registered", fs.Arg(0))
	} else if err != nil {
		return err
	}
	return nil
}

func clientCommand(ctx context.Context, args []string, stdout io.Writer) error {
	if len(args) > 0 && args[0] == "init" {
		return clientInit(args[1:], stdout)
	}
	return clientServe(ctx, args)
}

// clientServe keeps the link to the hub up until ctx is done. Downlink
// messages land in the inbox; nothing hands them on yet.
func clientServe(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("fednet client", flag.ContinueOnError)
	hubURL := fs.String("hub", "", "hub base URL, such as http://fednet-hub:8080 (required)")
	dbPath := fs.String("db", "", "client database file (required)")
	credPath := fs.String("credential", "", "credential file written by client init (required)")
	if err := parseFlags(fs, args, 0); err != nil {
		return err
	}
	if *hubURL == "" || *dbPath == "" || *credPath == "" {
		return usageError("fednet client: -hub, -db and -credential are required")
	}
	cred, err := auth.Read(*credPath)
	if err != nil {
		return err
	}
	st, err := store.OpenClient(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	c := &link.Client{Store: st, ID: cred.ClientID, Hub: *hubURL, Header: cred.Header(version)}
	c.Run(ctx)
	return nil
}

// clientInit creates the credential file and prints the client id and the
// hash to register on the hub. The credential itself is printed nowhere.
func clientInit(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("fednet client init", flag.ContinueOnError)
	id := fs.String("id", "", "this client's id, as the hub will know it (required)")
	credPath := fs.String("credential", "", "credential file to create, mode 0600 (required)")
	if err := parseFlags(fs, args, 0); err != nil {
		return err
	}
	if *id == "" || *credPath == "" {
		return usageError("fednet client init: -id and -credential are required")
	}
	cred, err := auth.New(*id)
	if err != nil {
		return err
	}
	if err := auth.Write(*credPath, cred); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "%s %x\n", cred.ClientID, cred.Hash())
	return nil
}
