// Command fednet bridges Slack threads and the coding agents on each machine.
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	iofs "io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Luolc/fednet/internal/alert"
	"github.com/Luolc/fednet/internal/approval"
	"github.com/Luolc/fednet/internal/auth"
	"github.com/Luolc/fednet/internal/files"
	"github.com/Luolc/fednet/internal/handoff"
	"github.com/Luolc/fednet/internal/hook"
	"github.com/Luolc/fednet/internal/hubapi"
	"github.com/Luolc/fednet/internal/inbound"
	"github.com/Luolc/fednet/internal/link"
	"github.com/Luolc/fednet/internal/local"
	"github.com/Luolc/fednet/internal/outbound"
	"github.com/Luolc/fednet/internal/payload"
	"github.com/Luolc/fednet/internal/release"
	"github.com/Luolc/fednet/internal/route"
	"github.com/Luolc/fednet/internal/slack"
	"github.com/Luolc/fednet/internal/store"
	"github.com/Luolc/fednet/internal/upgrade"
	"github.com/Luolc/fednet/internal/watch"
)

// version is set at build time with -ldflags "-X main.version=v...".
var version = "dev"

// minClientVersion is the oldest release whose clients this hub serves;
// raise it when the hub stops understanding what older clients send. A
// client of the hub's own version is always served, "dev" included.
const minClientVersion = "v0.1.0"

// acceptVersion is link.Hub.AcceptVersion and watch.Watch.AcceptVersion.
func acceptVersion(client string) error {
	return release.Compatible(minClientVersion, client, version)
}

// newProcess makes the handoff.Process a hub or client runs as; tests that
// run several in one process use handoff.None.
var newProcess = func(timeout time.Duration) (handoff.Process, error) { return handoff.New(timeout) }

const usage = `usage: fednet <command> [flags]

commands:
  hub -listen ADDR -db PATH [-config PATH] [-admin-socket PATH] [-handoff-timeout D]
      [-slack-app-token-file PATH -slack-bot-token-file PATH] [-alert-webhook-file PATH]
      [-approval-key-file PATH] [-upgrade-request PATH]
        run the hub; with the two Slack token files it also takes in the
        messages people post in Slack, posts the clients' posts there and
        serves the files people upload to the clients, with the webhook
        file it sends alerts, and with the approval key (an Ed25519
        private key, PKCS#8 PEM) it runs approvals: posts the cards and
        signs what the approvers approve; each file holds one credential;
        the JSON config file says, for each channel, which client takes
        the threads people start in it and which clients may open threads
        in it, which client takes direct messages, lists the Slack users
        fednet serves, each with a name for the agents, which may be
        empty, names the channel approval cards go to and the users, from
        that list, who may approve, bounds the thread history a message
        that mentions the bot carries (how many messages, how many
        characters in all, how many in one message before it is cut; the
        defaults are shown), says which uploaded files a client fetches
        before it hands a message to its hook (the types, "image/*" for
        all images, the largest file, the most one message's files add up
        to) and the largest file a client may fetch at all, in bytes (the
        defaults are shown: 20 MiB, 50 MiB, 200 MiB), and names the users,
        from that list, who may upgrade with /fednet upgrade and whether
        the hub upgrades on its own when it finds a new release (default
        yes):
        {"channels": {"C123": {"machine": "CLIENT-ID", "open_thread": ["CLIENT-ID"]}},
         "dm": {"machine": "CLIENT-ID"}, "users": {"U123": "NAME"},
         "alerts": {"slack_down": "5m", "offline_queued": "10m"},
         "approvals": {"channel": "C456", "approvers": ["U123"]},
         "history": {"max_messages": 10, "max_chars": 4000, "max_message_chars": 2000},
         "files": {"prefetch_types": ["image/*", "application/pdf"], "prefetch_max_bytes": 20971520,
                   "prefetch_max_total_bytes": 52428800, "fetch_max_bytes": 209715200},
         "upgrade": {"admins": ["U123"], "auto": true}}
        the admin socket takes hub handoff; D is how long a new process may
        take to become ready at a handoff; the upgrade request is the file
        the hub writes to ask the machine's upgrader (fednet upgrade, run
        by root) for a new release, without which the hub cannot upgrade
        itself
  hub handoff -socket PATH [-json]
        replace the running hub with a new process of the binary now at its
        path, without a gap; prints the versions handed off from and to
  hub register -db PATH CLIENT-ID HASH
        let a client connect; HASH is what its client init printed
  hub revoke -db PATH CLIENT-ID
        retire a client: its credential stops working
  hub reassign -db PATH FROM-ID TO-ID
        move every thread FROM-ID owns to TO-ID; prints how many moved
  client -hub URL -db PATH -credential PATH -socket PATH [-socket-group GROUP]
         [-hook-timeout D] [-hook-env NAME]... [-handoff-timeout D] [-upgrade-request PATH]
         [-files-dir DIR] [-files-retention D] [-files-max-total-bytes N] [-prefetch-timeout D]
         [COMMAND [ARG]...]
        run a client on an agent machine; agents reach it through the socket,
        which only this user and the members of GROUP can connect to; COMMAND
        runs for each message received, with the event file as its last
        argument, in an environment of just PATH, HOME and each -hook-env NAME;
        the upgrade request is the file the client writes when the hub tells
        it to upgrade, for the machine's upgrader (fednet upgrade, run by
        root); without it the hub's notices are dropped; the files people
        upload in Slack are fetched through the hub into DIR (default: files
        next to the database), under <file id>/<name>, the images and PDFs
        of a message before its hook runs (waiting at most -prefetch-timeout,
        default 30s) and the rest on fetch-file; a file is kept for
        -files-retention after it was last fetched (default 168h) and the
        files are kept under N bytes in all (default 2 GiB)
  client handoff -socket PATH [-json]
        replace the running client with a new process of the binary now at
        its path, without a gap; prints the versions handed off from and to
  client post -socket PATH -thread KEY [-json] [--] TEXT
        post TEXT to a thread; prints the msg_id once the client has queued it;
        put -- before a TEXT that starts with -
  client read-thread -socket PATH [-json] THREAD-KEY
        print the messages of a thread, read by the hub
  client open-thread -socket PATH -channel CHANNEL [-json] [--] TEXT
        start a thread in CHANNEL with TEXT and print its key; this machine owns it
  client threads -socket PATH [-json]
        print the keys of the threads this machine owns
  client adopt -socket PATH [-json] THREAD-KEY
        make this machine the owner of a thread: people's replies in it come here
  client channel-context get -socket PATH [-json] CHANNEL
        print a channel's description (its Slack purpose)
  client channel-context set -socket PATH -body-file FILE [-json] CHANNEL
        replace a channel's description with the contents of FILE
  client users -socket PATH [-json]
        print the user list: each user's Slack id and name
  client dm -socket PATH -user USER-ID [-json] [--] TEXT
        send TEXT as a direct message to a user on the user list
  client fetch-file -socket PATH [-json] FILE-ID
        fetch the Slack file with FILE-ID (the id in a message's files)
        through the hub into the client's files directory, unless it is
        there already, and print its path
  client request-approval -socket PATH -agent NAME -action FILE [-requester USER-ID] [-json] [--] TEXT
        ask for approval of an action: TEXT says what it does, FILE holds
        its parameters as JSON, NAME is the agent that will act; prints the
        approval id once the hub has posted the card; the outcome comes as
        a message of type approval through the hook, signed when approved;
        an action the card cannot show whole is refused; -requester is a
        note on the card, not checked
  client init -id CLIENT-ID -credential PATH
        create this machine's credential; prints CLIENT-ID and HASH, never the credential
  approval verify -pubkey PATH -approval PATH -action PATH -machine CLIENT-ID -agent NAME -used PATH
        check an approval the hub signed before acting on it: the signature,
        that it has not expired, that it is for this machine and agent and for
        the action file as given, and that it was not used before; then record
        it as used; exit 0 to act, 5 bad signature, 6 expired, 7 the target or
        the action differs, 8 already used, 1 a file cannot be read or the
        public key is not an ssh-ed25519 line, 2 the approval is not well formed
  upgrade -request PATH -binary PATH -socket PATH
        carry out the upgrade request at PATH, then delete it: download the
        release it names for this machine from fednet's releases, check it
        against the release's SHA256SUMS, put it at the binary's PATH in one
        rename, and hand the hub or client answering on the socket off to
        it; refuses a release that is not newer than what runs; for the root
        unit that watches the request file
  version
        print the version
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

// run is main without the process: it returns the exit code. 2 is a usage
// error, an exitError picks its own code, and 1 is any other failure.
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
	case "approval":
		cmd = func(_ context.Context, args []string, stdout io.Writer) error { return approvalCommand(args, stdout) }
	case "upgrade":
		cmd = upgradeCommand
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
	var eerr exitError
	switch {
	case err == nil:
		return 0
	case errors.As(err, &uerr):
		fmt.Fprintf(stderr, "fednet: %v\n%s", err, usage)
		return 2
	case errors.As(err, &eerr):
		fmt.Fprintf(stderr, "fednet: %v\n", err)
		return eerr.code
	default:
		fmt.Fprintf(stderr, "fednet: %v\n", err)
		return 1
	}
}

// usageError is a command-line mistake; run prints the usage after it.
type usageError string

func (e usageError) Error() string { return string(e) }

// Exit codes besides 0, 1 and the 2 of a usageError.
const (
	exitBadRequest  = 2 // the client daemon or the hub refused the request as malformed
	exitDenied      = 3 // not allowed to use the socket, or not allowed by the hub
	exitUnreachable = 4 // the client daemon or the hub cannot be reached
)

// exitCodes maps a failed request's local.Response.Kind to its exit code;
// any other failure is 1.
var exitCodes = map[string]int{
	local.BadRequest:  exitBadRequest,
	local.Denied:      exitDenied,
	local.Unreachable: exitUnreachable,
}

// exitError is a failure that run reports with its own exit code.
type exitError struct {
	code int
	err  error
}

func (e exitError) Error() string { return e.err.Error() }

// parseFlags parses args with fs and checks for exactly want positional
// arguments; want < 0 allows any number. A flag error is already reported
// by fs.
func parseFlags(fs *flag.FlagSet, args []string, want int) error {
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return usageError(fs.Name() + ": " + err.Error())
	}
	if want >= 0 && fs.NArg() != want {
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
		case "reassign":
			return hubReassign(ctx, args[1:], stdout)
		case "handoff":
			return handoffCommand(ctx, "fednet hub handoff", args[1:], stdout)
		}
	}
	return hubServe(ctx, args, stdout)
}

// hubConfig is the hub's config file.
type hubConfig struct {
	Channels map[string]struct {
		// Machine is the client that takes the threads people start in
		// the channel; empty when none does.
		Machine string `json:"machine"`
		// OpenThread lists the clients that may open threads in the channel.
		OpenThread []string `json:"open_thread"`
	} `json:"channels"`
	// DM is about direct messages to the bot.
	DM struct {
		// Machine is the client that takes the threads people start in
		// direct messages; empty when none does.
		Machine string `json:"machine"`
	} `json:"dm"`
	// Users maps the Slack user id of each person fednet serves to a name
	// for the agents, which may be empty.
	Users map[string]string `json:"users"`
	// Alerts sets the thresholds of the hub's alerts; a field left out
	// keeps the watch package's default.
	Alerts struct {
		// SlackDown is how long the hub may be cut off from Slack.
		SlackDown duration `json:"slack_down"`
		// OfflineQueued is how long a message may wait for an offline
		// client.
		OfflineQueued duration `json:"offline_queued"`
	} `json:"alerts"`
	// Approvals is about approval cards.
	Approvals struct {
		// Channel is where the cards go.
		Channel string `json:"channel"`
		// Approvers are the Slack user ids of the people who may decide;
		// each must be on Users.
		Approvers []string `json:"approvers"`
	} `json:"approvals"`
	// History bounds the thread history a message that mentions the bot
	// carries, and the length of any message's text; a field left out or
	// zero keeps the inbound package's default.
	History struct {
		MaxMessages     int `json:"max_messages"`
		MaxChars        int `json:"max_chars"`
		MaxMessageChars int `json:"max_message_chars"`
	} `json:"history"`
	// Files is about the files people upload in Slack; a field left out
	// or zero keeps the package's default.
	Files struct {
		// PrefetchTypes are the mimetypes of the files a client fetches
		// before it runs the hook; PrefetchMaxBytes the largest such
		// file and PrefetchMaxTotalBytes the most one message's add up
		// to.
		PrefetchTypes         []string `json:"prefetch_types"`
		PrefetchMaxBytes      int64    `json:"prefetch_max_bytes"`
		PrefetchMaxTotalBytes int64    `json:"prefetch_max_total_bytes"`
		// FetchMaxBytes is the largest file the hub serves to a client.
		FetchMaxBytes int64 `json:"fetch_max_bytes"`
	} `json:"files"`
	// Upgrade is about upgrades.
	Upgrade struct {
		// Admins are the Slack user ids of the people who may upgrade;
		// each must be on Users.
		Admins []string `json:"admins"`
		// Auto, unless set to false, makes the hub look for a new release
		// every hour and upgrade to it.
		Auto *bool `json:"auto"`
	} `json:"upgrade"`
}

// auto reports whether the hub upgrades on its own: yes unless the config
// says no.
func (c hubConfig) auto() bool { return c.Upgrade.Auto == nil || *c.Upgrade.Auto }

// duration is a time.Duration written in JSON as a string such as "5m".
type duration time.Duration

func (d *duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	if v <= 0 {
		return fmt.Errorf("duration %q is not positive", s)
	}
	*d = duration(v)
	return nil
}

// readHubConfig reads the config file at path. An unknown field is an
// error, so that a misspelt one does not silently deny, and so is anything
// after the config object.
func readHubConfig(path string) (hubConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return hubConfig{}, err
	}
	defer f.Close()
	d := json.NewDecoder(f)
	d.DisallowUnknownFields()
	var cfg hubConfig
	if err := d.Decode(&cfg); err != nil {
		return hubConfig{}, fmt.Errorf("%s: %w", path, err)
	}
	if err := d.Decode(&struct{}{}); err != io.EOF {
		return hubConfig{}, fmt.Errorf("%s: more than one JSON value", path)
	}
	for _, u := range cfg.Approvals.Approvers {
		if _, ok := cfg.Users[u]; !ok {
			return hubConfig{}, fmt.Errorf("%s: approver %s is not on the user list", path, u)
		}
	}
	for _, u := range cfg.Upgrade.Admins {
		if _, ok := cfg.Users[u]; !ok {
			return hubConfig{}, fmt.Errorf("%s: upgrade admin %s is not on the user list", path, u)
		}
	}
	if h := cfg.History; h.MaxMessages < 0 || h.MaxChars < 0 || h.MaxMessageChars < 0 {
		return hubConfig{}, fmt.Errorf("%s: history limits must not be negative", path)
	}
	if f := cfg.Files; f.PrefetchMaxBytes < 0 || f.PrefetchMaxTotalBytes < 0 || f.FetchMaxBytes < 0 {
		return hubConfig{}, fmt.Errorf("%s: files limits must not be negative", path)
	}
	return cfg, nil
}

// prefetch is the files part of the config, as the inbound package takes
// it.
func (c hubConfig) prefetch() inbound.Prefetch {
	return inbound.Prefetch{Types: c.Files.PrefetchTypes, MaxBytes: c.Files.PrefetchMaxBytes, MaxTotal: c.Files.PrefetchMaxTotalBytes}
}

// history is the limits of the thread history, as the inbound package
// takes them.
func (c hubConfig) history() inbound.Limits {
	return inbound.Limits{MaxMessages: c.History.MaxMessages, MaxChars: c.History.MaxChars, MaxMessageChars: c.History.MaxMessageChars}
}

// route is the routing part of the config: which client takes new threads
// in each channel and in direct messages.
func (c hubConfig) route() route.Config {
	defaults := make(map[string]string)
	for ch, cc := range c.Channels {
		if cc.Machine != "" {
			defaults[ch] = cc.Machine
		}
	}
	return route.Config{Defaults: defaults, DM: c.DM.Machine}
}

// openThread maps each channel to the clients that may open threads in it.
func (c hubConfig) openThread() map[string][]string {
	m := make(map[string][]string, len(c.Channels))
	for ch, cc := range c.Channels {
		m[ch] = cc.OpenThread
	}
	return m
}

// How the hub reaches Slack once it has the tokens; tests replace them
// with fakes.
var (
	newSlack   = func(botToken string) slack.API { return slack.New(botToken) }
	runInbound = inbound.Run
)

// Timings tests shorten; zero means each package's default.
var (
	hookRetry        hook.Retry
	outboundInterval time.Duration
	approvalInterval time.Duration
	approvalTTL      time.Duration
	upgradeTimings   struct{ interval, wait, poll, fetch time.Duration }
)

// releases is where the hub looks for new releases and the upgrader
// downloads them from; tests point it at a fake.
var releases = upgrade.GitHub

// readSecret reads the credential in the file at path, without the white
// space around it. No error quotes the contents.
func readSecret(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	s := strings.TrimSpace(string(b))
	if s == "" {
		return "", fmt.Errorf("%s is empty", path)
	}
	return s, nil
}

// hubServe runs the hub until ctx is done, or until a handoff has put a
// new process in its place. It prints the address it listens on, so that
// -listen with port 0 is usable. With the Slack tokens it also runs the
// inbound and outbound sides and, with the webhook, the alerts; when the
// Socket Mode connection fails for good, the HTTP server fails, or ctx is
// done, everything stops. After a handoff it stops the same way, once the
// requests in hand are answered. As the successor of a handoff it opens
// its own Socket Mode connection at once, but starts the outbound side and
// the alerts only once the predecessor has exited: they run in one
// process at a time.
func hubServe(ctx context.Context, args []string, stdout io.Writer) (err error) {
	fs := flag.NewFlagSet("fednet hub", flag.ContinueOnError)
	listen := fs.String("listen", "", "address to listen on, such as 127.0.0.1:8080 (required)")
	dbPath := fs.String("db", "", "hub database file (required)")
	configPath := fs.String("config", "", "hub config file; without one, no client may open threads")
	adminSocket := fs.String("admin-socket", "", "unix socket that takes hub handoff; without one the hub cannot hand off")
	handoffTimeout := fs.Duration("handoff-timeout", handoff.DefaultTimeout, "how long a new process may take to become ready at a handoff")
	appTokenPath := fs.String("slack-app-token-file", "", "file holding the Slack app-level token, for Socket Mode")
	botTokenPath := fs.String("slack-bot-token-file", "", "file holding the Slack bot token, for the Web API")
	webhookPath := fs.String("alert-webhook-file", "", "file holding the URL of the Slack incoming webhook for alerts; read only with the Slack token files")
	keyPath := fs.String("approval-key-file", "", "file holding the Ed25519 private key that signs approvals, PKCS#8 PEM; without it no approval can be requested")
	upgradeRequest := fs.String("upgrade-request", "", "file the hub writes to ask the machine's upgrader for a release; without it the hub cannot upgrade itself")
	if err := parseFlags(fs, args, 0); err != nil {
		return err
	}
	if *listen == "" || *dbPath == "" {
		return usageError("fednet hub: -listen and -db are required")
	}
	if (*appTokenPath == "") != (*botTokenPath == "") {
		return usageError("fednet hub: give both -slack-app-token-file and -slack-bot-token-file, or neither")
	}
	proc, err := newProcess(*handoffTimeout)
	if err != nil {
		return err
	}
	// Why this process did not start goes to the predecessor, if any.
	defer func() {
		if err != nil {
			proc.Report(err)
		}
	}()
	var cfg hubConfig
	if *configPath != "" {
		if cfg, err = readHubConfig(*configPath); err != nil {
			return err
		}
	}
	if *appTokenPath == "" {
		// Without Slack nothing sends alerts, so the webhook is not read.
		*webhookPath = ""
	}
	var appToken, botToken, webhookURL string
	for _, f := range []struct {
		path string
		v    *string
	}{{*appTokenPath, &appToken}, {*botTokenPath, &botToken}, {*webhookPath, &webhookURL}} {
		if f.path == "" {
			continue
		}
		if *f.v, err = readSecret(f.path); err != nil {
			return err
		}
	}
	var sl slack.API
	var bot string
	if botToken != "" {
		sl = newSlack(botToken)
		// Which user the bot is decides which messages mention it; a
		// hub that cannot find out serves no channel.
		if bot, err = sl.Self(ctx); err != nil {
			return fmt.Errorf("asking Slack which user the bot is: %w", err)
		}
	}
	var webhook *alert.Webhook
	if webhookURL != "" {
		webhook = &alert.Webhook{URL: webhookURL, From: inbound.HubName}
	}
	var key ed25519.PrivateKey
	if *keyPath != "" {
		if key, err = approval.ReadPrivateKey(*keyPath); err != nil {
			return err
		}
	}
	st, err := store.OpenHub(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	poster := &outbound.Poster{Store: st, Slack: sl, Alert: webhook, Interval: outboundInterval}
	hub := &link.Hub{
		Store:         st,
		Identify:      (&auth.Authenticator{Store: st}).Identify,
		AcceptVersion: acceptVersion,
		Uplinked:      poster.Nudge,
	}
	// Without the key, or without Slack, the hub runs no approvals: a
	// request is refused rather than left waiting for a card that cannot
	// be posted or a signature that cannot be made.
	var approvals *approval.Flow
	if key != nil && sl != nil {
		approvals = &approval.Flow{
			Store: st, Slack: sl, Key: key, Channel: cfg.Approvals.Channel, Approvers: cfg.Approvals.Approvers,
			TTL: approvalTTL, Interval: approvalInterval, Stored: hub.WakeAll,
		}
	}
	api := &hubapi.Server{Store: st, Slack: sl, OpenThread: cfg.openThread(), Users: cfg.Users, Approvals: approvals, MaxFetchBytes: cfg.Files.FetchMaxBytes}
	hub.Answer, hub.Fetch = api.Answer, api.Fetch
	upgrades := &upgrade.Hub{
		Store: st, Version: version, Releases: releases, Online: hub.Online, Slack: sl, Request: *upgradeRequest, HandedOff: proc.Exit(),
		Users: cfg.Users, Admins: cfg.Upgrade.Admins, Auto: cfg.auto(),
		Interval: upgradeTimings.interval, Wait: upgradeTimings.wait, Poll: upgradeTimings.poll, Fetch: upgradeTimings.fetch,
		Send: func(ctx context.Context, client string, payload []byte) error {
			_, err := hub.Send(ctx, client, payload)
			return err
		},
	}
	if webhook != nil {
		upgrades.Alert = webhook.Send
	}
	hub.UpgradeTo = upgrades.UpgradeTo
	ln, err := proc.ListenTCP(*listen)
	if err != nil {
		return err
	}
	var admin net.Listener
	if *adminSocket != "" {
		if admin, err = proc.ListenUnix(*adminSocket, ""); err != nil {
			ln.Close()
			return err
		}
	}
	fmt.Fprintf(stdout, "listening on %s\n", ln.Addr())
	srv := &http.Server{Handler: hub.Handler(), ReadHeaderTimeout: 10 * time.Second}
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	adminServed := make(chan error, 1)
	if admin == nil {
		adminServed <- nil
	} else {
		go func() { adminServed <- (&local.Server{Handoff: proc.Handoff, Version: version}).Serve(ctx, admin) }()
	}
	var wg sync.WaitGroup
	failed := make(chan error, 1)
	var r *inbound.Receiver
	if sl == nil {
		slog.Info("hub: Slack not configured, serving the clients only")
	} else {
		r = &inbound.Receiver{Store: st, Slack: sl, Route: cfg.route(), Users: cfg.Users, Bot: bot, History: cfg.history(), Prefetch: cfg.prefetch(), Approvals: approvals, Commands: upgrades, Stored: func() {
			hub.WakeAll()
			poster.Nudge()
		}}
		wg.Go(func() {
			// Run returns before ctx is done only when Slack rejects the
			// app token.
			if err := runInbound(ctx, appToken, r, 0); ctx.Err() == nil {
				failed <- fmt.Errorf("slack socket mode: %w", err)
			}
		})
	}
	// Ready before the predecessor is told to go: a failure here means
	// this process exits and the predecessor stays. A successor with Slack
	// is ready only once its own connection is up: a token Slack rejects
	// must fail the handoff, not the service.
	err = nil
	if r != nil && proc.HasParent() {
		select {
		case <-r.Up():
		case err = <-failed:
		case <-ctx.Done():
			err = ctx.Err()
		}
	}
	if err == nil {
		err = proc.Ready()
	}
	if err != nil {
		cancel()
		<-adminServed
		srv.Close()
		<-served
		wg.Wait()
		return err
	}
	// posted is closed once the outbound side has stopped, or will never
	// start; upgraded once the upgrades have, or will never start.
	posted := make(chan struct{})
	upgraded := make(chan struct{})
	wg.Go(func() {
		if err := proc.WaitForParent(ctx); err != nil {
			if ctx.Err() != nil {
				close(posted)
				close(upgraded)
				return
			}
			slog.Warn("hub: the predecessor misbehaved on exit", "err", err)
		}
		// The upgrades run in one process at a time too: a check for a
		// new release by the predecessor and the successor both would
		// start the same upgrade twice.
		wg.Go(func() {
			defer close(upgraded)
			upgrades.Run(ctx)
		})
		if sl == nil {
			close(posted)
			return
		}
		wg.Go(func() {
			defer close(posted)
			poster.Run(ctx)
		})
		if approvals == nil {
			slog.Info("hub: no approval key, approvals are off")
		} else {
			wg.Go(func() { approvals.Run(ctx) })
		}
		if webhook == nil {
			slog.Warn("hub: no alert webhook, alerts are only logged")
			return
		}
		w := &watch.Watch{
			Store: st, Slack: sl, Link: r, Online: hub.Online, AcceptVersion: acceptVersion, Alert: webhook,
			SlackDown: time.Duration(cfg.Alerts.SlackDown), Offline: time.Duration(cfg.Alerts.OfflineQueued),
		}
		wg.Go(func() { w.Run(ctx) })
	})
	stopped, handedOff := false, false
	select {
	case err = <-served:
		stopped = true
	case err = <-failed:
	case <-ctx.Done():
	case <-proc.Exit():
		handedOff = true
	}
	if handedOff {
		// The successor accepts from now on. What this process has in hand
		// it finishes first: the requests, the handoff request among them,
		// the post with Slack, which the successor must not post again,
		// and the summary of an upgrade that waited for this very handoff.
		admin.Close()
		<-adminServed
		poster.Stop()
		<-posted
		<-upgraded
	} else {
		proc.Stop()
		cancel()
		if aerr := <-adminServed; err == nil {
			err = aerr
		}
	}
	cancel()
	// The downlink connections are hijacked WebSockets, which Shutdown
	// does not know about; the hub closes them itself.
	hub.Close()
	sctx, scancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer scancel()
	if serr := srv.Shutdown(sctx); err == nil {
		err = serr
	}
	if !stopped {
		<-served
	}
	wg.Wait()
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

// hubReassign moves every thread one client owns to another, as when a
// machine is replaced, and prints how many it moved.
func hubReassign(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("fednet hub reassign", flag.ContinueOnError)
	dbPath := fs.String("db", "", "hub database file (required)")
	if err := parseFlags(fs, args, 2); err != nil {
		return err
	}
	if *dbPath == "" || fs.Arg(0) == "" || fs.Arg(1) == "" {
		return usageError("fednet hub reassign: -db, FROM-ID and TO-ID are required")
	}
	st, err := store.OpenHub(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	n, err := st.ReassignClient(ctx, fs.Arg(0), fs.Arg(1))
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, n)
	return err
}

func clientCommand(ctx context.Context, args []string, stdout io.Writer) error {
	if len(args) > 0 {
		switch args[0] {
		case "init":
			return clientInit(args[1:], stdout)
		case "post":
			return clientPost(ctx, args[1:], stdout)
		case "read-thread":
			return clientReadThread(ctx, args[1:], stdout)
		case "open-thread":
			return clientOpenThread(ctx, args[1:], stdout)
		case "threads":
			return clientThreads(ctx, args[1:], stdout)
		case "adopt":
			return clientAdopt(ctx, args[1:], stdout)
		case "channel-context":
			return clientChannelContext(ctx, args[1:], stdout)
		case "users":
			return clientUsers(ctx, args[1:], stdout)
		case "dm":
			return clientDM(ctx, args[1:], stdout)
		case "handoff":
			return handoffCommand(ctx, "fednet client handoff", args[1:], stdout)
		case "request-approval":
			return clientRequestApproval(ctx, args[1:], stdout)
		case "fetch-file":
			return clientFetchFile(ctx, args[1:], stdout)
		}
	}
	return clientServe(ctx, args)
}

// hookEnv is the client's environment variables that the hook always gets.
var hookEnv = []string{"PATH", "HOME"}

// clientServe keeps the link to the hub up and answers the agents on the
// socket until ctx is done, or until a handoff has put a new process in its
// place. Downlink messages land in the inbox, and the hook command, if
// given, hands each one to the agent. The predecessor's link and hook run
// until it has exited, so this process starts them only then; the socket
// it serves from the start.
func clientServe(ctx context.Context, args []string) (err error) {
	fs := flag.NewFlagSet("fednet client", flag.ContinueOnError)
	hubURL := fs.String("hub", "", "hub base URL, such as http://fednet-hub:8080 (required)")
	dbPath := fs.String("db", "", "client database file (required)")
	credPath := fs.String("credential", "", "credential file written by client init (required)")
	socket := fs.String("socket", "", "unix socket for the agents (required)")
	group := fs.String("socket-group", "", "group whose members may use the socket")
	hookTimeout := fs.Duration("hook-timeout", hook.DefaultTimeout, "how long one run of the hook may take")
	handoffTimeout := fs.Duration("handoff-timeout", handoff.DefaultTimeout, "how long a new process may take to become ready at a handoff")
	upgradeRequest := fs.String("upgrade-request", "", "file the client writes when the hub tells it to upgrade, for the machine's upgrader; without it the notices are dropped")
	filesDir := fs.String("files-dir", "", "where the files people upload in Slack are kept; default: files next to the database")
	filesRetention := fs.Duration("files-retention", files.DefaultRetention, "how long a fetched file is kept after it was last fetched")
	filesMaxTotal := fs.Int64("files-max-total-bytes", files.DefaultMaxTotal, "the most the fetched files add up to, in bytes")
	prefetchTimeout := fs.Duration("prefetch-timeout", files.DefaultTimeout, "how long fetching one message's files before its hook runs may take")
	env := hookEnv
	fs.Func("hook-env", "environment variable to pass to the hook (repeatable)", func(name string) error {
		env = append(env, name)
		return nil
	})
	if err := parseFlags(fs, args, -1); err != nil {
		return err
	}
	if *hubURL == "" || *dbPath == "" || *credPath == "" || *socket == "" {
		return usageError("fednet client: -hub, -db, -credential and -socket are required")
	}
	if *filesRetention <= 0 || *filesMaxTotal <= 0 || *prefetchTimeout <= 0 {
		return usageError("fednet client: -files-retention, -files-max-total-bytes and -prefetch-timeout must be positive")
	}
	if *filesDir == "" {
		*filesDir = filepath.Join(filepath.Dir(*dbPath), "files")
	}
	proc, err := newProcess(*handoffTimeout)
	if err != nil {
		return err
	}
	// Why this process did not start goes to the predecessor, if any.
	defer func() {
		if err != nil {
			proc.Report(err)
		}
	}()
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
	u := &upgrade.Client{Version: version, Request: *upgradeRequest, Alert: uplinkAlert(c)}
	c.Divert, c.Upgrade = u.Divert, u.Notice
	cache := &files.Store{Dir: *filesDir, Fetch: c.Fetch, Retention: *filesRetention, MaxTotal: *filesMaxTotal, Timeout: *prefetchTimeout}
	ln, err := proc.ListenUnix(*socket, *group)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	served := make(chan error, 1)
	go func() {
		served <- (&local.Server{Post: c.Post, Request: c.Request, Handoff: proc.Handoff, Version: version, Fetch: cache.Get}).Serve(ctx, ln)
	}()
	// Ready before the predecessor is told to go: a failure here means
	// this process exits and the predecessor stays.
	if err := proc.Ready(); err != nil {
		cancel()
		<-served
		return err
	}
	var h *hook.Runner
	if fs.NArg() > 0 {
		h = &hook.Runner{Store: st, Command: fs.Args(), Env: passEnv(env), Timeout: *hookTimeout, Retry: hookRetry, Alert: uplinkAlert(c), Prepare: cache.Attach, Prune: func() {
			if err := cache.Prune(); err != nil {
				slog.Warn("client: pruning the files", "err", err)
			}
		}}
		c.Received = h.Nudge
	}
	// hooked is closed once the hook has stopped, or will never start.
	hooked := make(chan struct{})
	linked := make(chan struct{})
	go func() {
		defer close(linked)
		if err := proc.WaitForParent(ctx); err != nil {
			if ctx.Err() != nil {
				close(hooked)
				return
			}
			slog.Warn("client: the predecessor misbehaved on exit", "err", err)
		}
		if h == nil {
			close(hooked)
		} else {
			go func() {
				defer close(hooked)
				h.Run(ctx)
			}()
		}
		c.Run(ctx)
		<-hooked
	}()
	stopped, handedOff := false, false
	select {
	case err = <-served:
		stopped = true
	case <-ctx.Done():
	case <-proc.Exit():
		handedOff = true
	}
	if handedOff {
		// The successor accepts from now on. What this process has in hand
		// it finishes first: the requests, the handoff request among them,
		// and the run of the hook, which the successor must not run again.
		ln.Close()
		<-served
		if h != nil {
			h.Stop()
		}
		<-hooked
	} else {
		proc.Stop()
		cancel()
		if !stopped {
			if serr := <-served; err == nil {
				err = serr
			}
		}
	}
	cancel()
	<-linked
	return err
}

// handoffCommand asks the daemon on the socket to hand off to a new
// process, waits for the new process to answer on the socket, and prints
// the versions handed off from and to. A handoff that failed is an error:
// the old process is still serving.
func handoffCommand(ctx context.Context, name string, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	socket, asJSON := socketFlags(fs)
	if err := parseFlags(fs, args, 0); err != nil {
		return err
	}
	if *socket == "" {
		return usageError(name + ": -socket is required")
	}
	old, now, err := handOff(ctx, *socket)
	if err != nil {
		return err
	}
	if *asJSON {
		return json.NewEncoder(stdout).Encode(struct {
			From local.Response `json:"from"`
			To   local.Response `json:"to"`
		}{old, now})
	}
	_, err = fmt.Fprintf(stdout, "handed off from %s (pid %d) to %s (pid %d)\n", old.Version, old.PID, now.Version, now.PID)
	return err
}

// handOff asks the daemon on socket to hand off and waits for the new
// process to answer there; it returns the answers of the old process and
// the new. A handoff that failed is an error: the old process is still
// serving.
func handOff(ctx context.Context, socket string) (old, now local.Response, err error) {
	if old, err = do(ctx, socket, local.Request{Cmd: local.Handoff}); err != nil {
		return old, now, err
	}
	// The old process stops accepting once it has answered; until then a
	// request may still reach it.
	for deadline := time.Now().Add(local.Timeout); now.PID == 0 || now.PID == old.PID; {
		if now, err = do(ctx, socket, local.Request{Cmd: local.Version}); err != nil {
			return old, now, fmt.Errorf("the new process does not answer on %s: %w", socket, err)
		}
		if now.PID != old.PID {
			break
		}
		if time.Now().After(deadline) {
			return old, now, fmt.Errorf("the old process (pid %d) still answers on %s", old.PID, socket)
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			return old, now, ctx.Err()
		}
	}
	return old, now, nil
}

// upgradeCommand carries out the upgrade request at -request: it reads the
// release the request names and deletes the request first of all, so that
// the unit watching it is not started again whatever happens next; asks
// the daemon on -socket what it runs and refuses a release that is not
// newer; downloads and checks the release's binary for this machine and
// puts it at -binary in one rename, keeping the old one; then hands the
// daemon off to it and prints the versions handed off from and to. A
// handoff that fails puts the old binary back, so that the next start of
// the service runs what is known to work. What became of the request is
// written next to it, for the hub to read.
func upgradeCommand(ctx context.Context, args []string, stdout io.Writer) (err error) {
	fs := flag.NewFlagSet("fednet upgrade", flag.ContinueOnError)
	request := fs.String("request", "", "the upgrade request file (required)")
	binary := fs.String("binary", "", "the binary to replace, which the daemon runs (required)")
	socket := fs.String("socket", "", "the socket the daemon hands off on: the hub's admin socket or the client's socket (required)")
	if err := parseFlags(fs, args, 0); err != nil {
		return err
	}
	if *request == "" || *binary == "" || *socket == "" {
		return usageError("fednet upgrade: -request, -binary and -socket are required")
	}
	target, err := upgrade.ReadRequest(*request)
	if errors.Is(err, upgrade.ErrNoRequest) {
		fmt.Fprintf(stdout, "no upgrade request at %s\n", *request)
		return nil
	}
	if rerr := os.Remove(*request); rerr != nil && err == nil {
		err = fmt.Errorf("deleting the request: %w", rerr)
	}
	if err != nil {
		return err
	}
	defer func() {
		text := "ok " + target
		if err != nil {
			text = "failed " + target + ": " + err.Error()
		}
		if werr := upgrade.WriteResult(*request, text); werr != nil {
			slog.Warn("upgrade: writing the result", "err", werr)
		}
	}()
	running, err := do(ctx, *socket, local.Request{Cmd: local.Version})
	if err != nil {
		return fmt.Errorf("asking what runs on %s: %w", *socket, err)
	}
	if !release.Newer(target, running.Version) {
		return fmt.Errorf("refusing to upgrade: %s is not newer than the running %s", target, running.Version)
	}
	if err := releases.Install(ctx, target, runtime.GOARCH, *binary); err != nil {
		return err
	}
	slog.Info("upgrade: installed", "version", target, "binary", *binary)
	old, now, err := handOff(ctx, *socket)
	if err != nil {
		if rerr := upgrade.Restore(*binary); rerr != nil {
			return fmt.Errorf("the handoff to %s failed: %w; and %v, so %s is at %s while the running %s keeps serving", target, err, rerr, target, *binary, running.Version)
		}
		return fmt.Errorf("the handoff to %s failed, rolled back to %s, which keeps serving: %w", target, running.Version, err)
	}
	if err := os.Remove(upgrade.Previous(*binary)); err != nil {
		slog.Warn("upgrade: removing the old binary", "err", err)
	}
	_, err = fmt.Fprintf(stdout, "upgraded from %s (pid %d) to %s (pid %d)\n", old.Version, old.PID, now.Version, now.PID)
	return err
}

// uplinkAlert returns an alert that goes up to the hub, which sends it to
// the alerts webhook: the webhook's URL is kept on the hub alone.
func uplinkAlert(c *link.Client) func(ctx context.Context, text string) error {
	return func(ctx context.Context, text string) error {
		b, err := json.Marshal(payload.Message{Type: payload.Alert, Text: text})
		if err != nil {
			return err
		}
		_, err = c.Post(ctx, b)
		return err
	}
}

// clientPost hands a post to the client daemon through its socket and
// prints the msg_id. It does not wait for the hub, and does not open the
// database: only the daemon does.
func clientPost(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("fednet client post", flag.ContinueOnError)
	socket, asJSON := socketFlags(fs)
	thread := fs.String("thread", "", "key of the thread to post to (required)")
	if err := parseFlags(fs, args, 1); err != nil {
		return err
	}
	if *socket == "" || *thread == "" || fs.Arg(0) == "" {
		return usageError("fednet client post: -socket, -thread and a non-empty TEXT are required")
	}
	res, err := do(ctx, *socket, local.Request{Cmd: local.Post, Thread: *thread, Text: fs.Arg(0)})
	if err != nil {
		return err
	}
	if *asJSON {
		return json.NewEncoder(stdout).Encode(res)
	}
	_, err = fmt.Fprintln(stdout, res.MsgID)
	return err
}

// passEnv returns the named variables of this process's environment, as
// KEY=VALUE, skipping the ones not set.
func passEnv(names []string) []string {
	env := []string{}
	for _, name := range names {
		if v, ok := os.LookupEnv(name); ok {
			env = append(env, name+"="+v)
		}
	}
	return env
}

// do sends req to the client daemon's socket. A request that could not be
// sent, or that failed, is an error with the exit code for why.
func do(ctx context.Context, socket string, req local.Request) (local.Response, error) {
	res, err := local.Do(ctx, socket, req)
	switch {
	case errors.Is(err, iofs.ErrPermission):
		return res, exitError{exitDenied, err}
	case err != nil:
		return res, exitError{exitUnreachable, err}
	case res.Error != "":
		if code, ok := exitCodes[res.Kind]; ok {
			return res, exitError{code, errors.New(res.Error)}
		}
		return res, errors.New(res.Error)
	}
	return res, nil
}

// socketFlags declares the flags every command that talks to the client
// daemon takes.
func socketFlags(fs *flag.FlagSet) (socket *string, asJSON *bool) {
	return fs.String("socket", "", "the client daemon's socket (required)"),
		fs.Bool("json", false, "print the reply as JSON")
}

// clientReadThread prints the messages of a thread, which the hub reads
// from Slack.
func clientReadThread(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("fednet client read-thread", flag.ContinueOnError)
	socket, asJSON := socketFlags(fs)
	if err := parseFlags(fs, args, 1); err != nil {
		return err
	}
	if *socket == "" || fs.Arg(0) == "" {
		return usageError("fednet client read-thread: -socket and THREAD-KEY are required")
	}
	res, err := do(ctx, *socket, local.Request{Cmd: hubapi.ReadThread, Thread: fs.Arg(0)})
	if err != nil {
		return err
	}
	if *asJSON {
		return json.NewEncoder(stdout).Encode(res)
	}
	for _, m := range res.Messages {
		if _, err := fmt.Fprintf(stdout, "%s %s: %s\n", m.TS, m.User, m.Text); err != nil {
			return err
		}
	}
	return nil
}

// clientOpenThread starts a thread through the hub and prints its key.
func clientOpenThread(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("fednet client open-thread", flag.ContinueOnError)
	socket, asJSON := socketFlags(fs)
	channel := fs.String("channel", "", "channel to start the thread in (required)")
	if err := parseFlags(fs, args, 1); err != nil {
		return err
	}
	if *socket == "" || *channel == "" || fs.Arg(0) == "" {
		return usageError("fednet client open-thread: -socket, -channel and a non-empty TEXT are required")
	}
	res, err := do(ctx, *socket, local.Request{Cmd: hubapi.OpenThread, Channel: *channel, Text: fs.Arg(0)})
	if err != nil {
		return err
	}
	if *asJSON {
		return json.NewEncoder(stdout).Encode(res)
	}
	_, err = fmt.Fprintln(stdout, res.Thread)
	return err
}

// clientThreads prints the threads this client owns, one key a line.
func clientThreads(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("fednet client threads", flag.ContinueOnError)
	socket, asJSON := socketFlags(fs)
	if err := parseFlags(fs, args, 0); err != nil {
		return err
	}
	if *socket == "" {
		return usageError("fednet client threads: -socket is required")
	}
	res, err := do(ctx, *socket, local.Request{Cmd: hubapi.Threads})
	if err != nil {
		return err
	}
	if *asJSON {
		return json.NewEncoder(stdout).Encode(res)
	}
	for _, t := range res.Threads {
		if _, err := fmt.Fprintln(stdout, t); err != nil {
			return err
		}
	}
	return nil
}

// clientAdopt makes this client the owner of a thread.
func clientAdopt(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("fednet client adopt", flag.ContinueOnError)
	socket, asJSON := socketFlags(fs)
	if err := parseFlags(fs, args, 1); err != nil {
		return err
	}
	if *socket == "" || fs.Arg(0) == "" {
		return usageError("fednet client adopt: -socket and THREAD-KEY are required")
	}
	res, err := do(ctx, *socket, local.Request{Cmd: hubapi.Adopt, Thread: fs.Arg(0)})
	if err != nil || !*asJSON {
		return err
	}
	return json.NewEncoder(stdout).Encode(res)
}

// clientChannelContext reads or replaces a channel's description.
func clientChannelContext(ctx context.Context, args []string, stdout io.Writer) error {
	if len(args) == 0 || (args[0] != "get" && args[0] != "set") {
		return usageError("fednet client channel-context: want get or set")
	}
	fs := flag.NewFlagSet("fednet client channel-context "+args[0], flag.ContinueOnError)
	socket, asJSON := socketFlags(fs)
	var bodyFile *string
	if args[0] == "set" {
		bodyFile = fs.String("body-file", "", "file holding the new description (required)")
	}
	if err := parseFlags(fs, args[1:], 1); err != nil {
		return err
	}
	if *socket == "" || fs.Arg(0) == "" || (bodyFile != nil && *bodyFile == "") {
		return usageError(fs.Name() + ": -socket, CHANNEL and, for set, -body-file are required")
	}
	req := local.Request{Cmd: hubapi.GetChannelContext, Channel: fs.Arg(0)}
	if bodyFile != nil {
		body, err := os.ReadFile(*bodyFile)
		if err != nil {
			return err
		}
		req.Cmd, req.Text = hubapi.SetChannelContext, string(body)
	}
	res, err := do(ctx, *socket, req)
	switch {
	case err != nil:
		return err
	case *asJSON:
		return json.NewEncoder(stdout).Encode(res)
	case req.Cmd == hubapi.GetChannelContext:
		_, err = fmt.Fprintln(stdout, res.Text)
	}
	return err
}

// clientUsers prints the user list, one user a line: the id, then the name
// if there is one.
func clientUsers(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("fednet client users", flag.ContinueOnError)
	socket, asJSON := socketFlags(fs)
	if err := parseFlags(fs, args, 0); err != nil {
		return err
	}
	if *socket == "" {
		return usageError("fednet client users: -socket is required")
	}
	res, err := do(ctx, *socket, local.Request{Cmd: hubapi.Users})
	if err != nil {
		return err
	}
	if *asJSON {
		return json.NewEncoder(stdout).Encode(res)
	}
	for _, u := range res.Users {
		if _, err := fmt.Fprintln(stdout, strings.TrimSpace(u.ID+" "+u.Name)); err != nil {
			return err
		}
	}
	return nil
}

// clientDM sends a direct message to a user on the user list through the
// hub, and waits for the hub to send it.
func clientDM(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("fednet client dm", flag.ContinueOnError)
	socket, asJSON := socketFlags(fs)
	user := fs.String("user", "", "Slack id of the user to message (required)")
	if err := parseFlags(fs, args, 1); err != nil {
		return err
	}
	if *socket == "" || *user == "" || fs.Arg(0) == "" {
		return usageError("fednet client dm: -socket, -user and a non-empty TEXT are required")
	}
	res, err := do(ctx, *socket, local.Request{Cmd: hubapi.DM, User: *user, Text: fs.Arg(0)})
	if err != nil || !*asJSON {
		return err
	}
	return json.NewEncoder(stdout).Encode(res)
}

// clientRequestApproval asks the hub for approval of an action and prints
// the approval id. The action file goes to the hub byte for byte: its
// hash is what the approval will be signed over.
func clientRequestApproval(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("fednet client request-approval", flag.ContinueOnError)
	socket, asJSON := socketFlags(fs)
	agent := fs.String("agent", "", "the agent that will act (required)")
	actionPath := fs.String("action", "", "file holding the action's parameters as JSON (required)")
	requester := fs.String("requester", "", "Slack id of the person the agent asks on behalf of, shown on the card as the agent's own word")
	if err := parseFlags(fs, args, 1); err != nil {
		return err
	}
	if *socket == "" || *agent == "" || *actionPath == "" || fs.Arg(0) == "" {
		return usageError("fednet client request-approval: -socket, -agent, -action and a non-empty TEXT are required")
	}
	action, err := os.ReadFile(*actionPath)
	if err != nil {
		return err
	}
	res, err := do(ctx, *socket, local.Request{Cmd: hubapi.RequestApproval, Agent: *agent, Requester: *requester, Action: action, Text: fs.Arg(0)})
	if err != nil {
		return err
	}
	if *asJSON {
		return json.NewEncoder(stdout).Encode(res)
	}
	_, err = fmt.Fprintln(stdout, res.ApprovalID)
	return err
}

// clientFetchFile has the client daemon fetch a file through the hub and
// prints where it is.
func clientFetchFile(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("fednet client fetch-file", flag.ContinueOnError)
	socket, asJSON := socketFlags(fs)
	if err := parseFlags(fs, args, 1); err != nil {
		return err
	}
	if *socket == "" || fs.Arg(0) == "" {
		return usageError("fednet client fetch-file: -socket and FILE-ID are required")
	}
	res, err := do(ctx, *socket, local.Request{Cmd: local.FetchFile, File: fs.Arg(0)})
	if err != nil {
		return err
	}
	if *asJSON {
		return json.NewEncoder(stdout).Encode(res)
	}
	_, err = fmt.Fprintln(stdout, res.Path)
	return err
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
