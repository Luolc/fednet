// Command fednet bridges Slack threads and the coding agents on each machine.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

const usage = `usage: fednet <command>

commands:
  hub      run the hub (not implemented yet)
  client   run a client on an agent machine (not implemented yet)
  version  print the version
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("fednet", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, usage) }
	if err := fs.Parse(args); err != nil {
		return 2
	}
	switch fs.Arg(0) {
	case "hub", "client":
		fmt.Fprintf(stderr, "fednet %s: not implemented yet\n", fs.Arg(0))
		return 2
	case "version":
		fmt.Fprintln(stdout, version)
		return 0
	default:
		fs.Usage()
		return 2
	}
}
