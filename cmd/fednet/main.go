// Command fednet bridges Slack threads and the coding agents on each machine.
package main

import (
	"context"
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
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Luolc/fednet/internal/alert"
	"github.com/Luolc/fednet/internal/auth"
	"github.com/Luolc/fednet/internal/hook"
	"github.com/Luolc/fednet/internal/hubapi"
	"github.com/Luolc/fednet/internal/inbound"
	"github.com/Luolc/fednet/internal/link"
	"github.com/Luolc/fednet/internal/local"
	"github.com/Luolc/fednet/internal/outbound"
	"github.com/Luolc/fednet/internal/payload"
	"github.com/Luolc/fednet/internal/route"
	"github.com/Luolc/fednet/internal/slack"
	"github.com/Luolc/fednet/internal/store"
	"github.com/Luolc/fednet/internal/watch"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

const usage = `usage: fednet <command> [flags]

commands:
  hub -listen ADDR -db PATH [-config PATH]
      [-slack-app-token-file PATH -slack-bot-token-file PATH] [-alert-webhook-file PATH]
        run the hub; with the two Slack token files it also takes in the
        messages people post in Slack and posts the clients' posts there,
        and with the webhook file it sends alerts; each file holds one
        credential; the JSON config file says, for each channel, which
        client takes the threads people start in it and which clients may
        open threads in it, which client takes direct messages, and lists
        the Slack users fednet serves, each with a name for the agents,
        which may be empty:
        {"channels": {"C123": {"machine": "CLIENT-ID", "open_thread": ["CLIENT-ID"]}},
         "dm": {"machine": "CLIENT-ID"}, "users": {"U123": "NAME"},
         "alerts": {"slack_down": "5m", "offline_queued": "10m"}}
  hub register -db PATH CLIENT-ID HASH
        let a client connect; HASH is what its client init printed
  hub revoke -db PATH CLIENT-ID
        retire a client: its credential stops working
  hub reassign -db PATH FROM-ID TO-ID
        move every thread FROM-ID owns to TO-ID; prints how many moved
  client -hub URL -db PATH -credential PATH -socket PATH [-socket-group GROUP]
         [-hook-timeout D] [-hook-env NAME]... [COMMAND [ARG]...]
        run a client on an agent machine; agents reach it through the socket,
        which only this user and the members of GROUP can connect to; COMMAND
        runs for each message received, with the event file as its last
        argument, in an environment of just PATH, HOME and each -hook-env NAME
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
}

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
	return cfg, nil
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
)

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

// hubServe runs the hub until ctx is done. It prints the address it listens
// on, so that -listen with port 0 is usable. With the Slack tokens it also
// runs the inbound and outbound sides and, with the webhook, the alerts;
// when the Socket Mode connection fails for good, the HTTP server fails,
// or ctx is done, everything stops.
func hubServe(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("fednet hub", flag.ContinueOnError)
	listen := fs.String("listen", "", "address to listen on, such as 127.0.0.1:8080 (required)")
	dbPath := fs.String("db", "", "hub database file (required)")
	configPath := fs.String("config", "", "hub config file; without one, no client may open threads")
	appTokenPath := fs.String("slack-app-token-file", "", "file holding the Slack app-level token, for Socket Mode")
	botTokenPath := fs.String("slack-bot-token-file", "", "file holding the Slack bot token, for the Web API")
	webhookPath := fs.String("alert-webhook-file", "", "file holding the URL of the Slack incoming webhook for alerts; read only with the Slack token files")
	if err := parseFlags(fs, args, 0); err != nil {
		return err
	}
	if *listen == "" || *dbPath == "" {
		return usageError("fednet hub: -listen and -db are required")
	}
	if (*appTokenPath == "") != (*botTokenPath == "") {
		return usageError("fednet hub: give both -slack-app-token-file and -slack-bot-token-file, or neither")
	}
	var cfg hubConfig
	if *configPath != "" {
		var err error
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
		var err error
		if *f.v, err = readSecret(f.path); err != nil {
			return err
		}
	}
	var sl slack.API
	if botToken != "" {
		sl = newSlack(botToken)
	}
	var webhook *alert.Webhook
	if webhookURL != "" {
		webhook = &alert.Webhook{URL: webhookURL, From: inbound.HubName}
	}
	st, err := store.OpenHub(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	poster := &outbound.Poster{Store: st, Slack: sl, Alert: webhook, Interval: outboundInterval}
	hub := &link.Hub{
		Store:    st,
		Identify: (&auth.Authenticator{Store: st}).Identify,
		Answer:   (&hubapi.Server{Store: st, Slack: sl, OpenThread: cfg.openThread(), Users: cfg.Users}).Answer,
		Uplinked: poster.Nudge,
	}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "listening on %s\n", ln.Addr())
	srv := &http.Server{Handler: hub.Handler(), ReadHeaderTimeout: 10 * time.Second}
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	failed := make(chan error, 1)
	if sl == nil {
		slog.Info("hub: Slack not configured, serving the clients only")
	} else {
		r := &inbound.Receiver{Store: st, Slack: sl, Route: cfg.route(), Users: cfg.Users, Stored: func() {
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
		wg.Go(func() { poster.Run(ctx) })
		if webhook == nil {
			slog.Warn("hub: no alert webhook, alerts are only logged")
		} else {
			w := &watch.Watch{
				Store: st, Slack: sl, Link: r, Online: hub.Online, Alert: webhook,
				SlackDown: time.Duration(cfg.Alerts.SlackDown), Offline: time.Duration(cfg.Alerts.OfflineQueued),
			}
			wg.Go(func() { w.Run(ctx) })
		}
	}
	stopped := false
	select {
	case err = <-served:
		stopped = true
	case err = <-failed:
	case <-ctx.Done():
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
		}
	}
	return clientServe(ctx, args)
}

// hookEnv is the client's environment variables that the hook always gets.
var hookEnv = []string{"PATH", "HOME"}

// clientServe keeps the link to the hub up and answers the agents on the
// socket until ctx is done. Downlink messages land in the inbox, and the
// hook command, if given, hands each one to the agent.
func clientServe(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("fednet client", flag.ContinueOnError)
	hubURL := fs.String("hub", "", "hub base URL, such as http://fednet-hub:8080 (required)")
	dbPath := fs.String("db", "", "client database file (required)")
	credPath := fs.String("credential", "", "credential file written by client init (required)")
	socket := fs.String("socket", "", "unix socket for the agents (required)")
	group := fs.String("socket-group", "", "group whose members may use the socket")
	hookTimeout := fs.Duration("hook-timeout", hook.DefaultTimeout, "how long one run of the hook may take")
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
	ln, err := local.Listen(*socket, *group)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	served := make(chan error, 1)
	go func() {
		served <- (&local.Server{Post: c.Post, Request: c.Request}).Serve(ctx, ln)
		cancel()
	}()
	hooked := make(chan struct{})
	if fs.NArg() == 0 {
		close(hooked)
	} else {
		h := &hook.Runner{Store: st, Command: fs.Args(), Env: passEnv(env), Timeout: *hookTimeout, Retry: hookRetry, Alert: uplinkAlert(c)}
		c.Received = h.Nudge
		go func() {
			defer close(hooked)
			h.Run(ctx)
		}()
	}
	c.Run(ctx)
	err = <-served
	<-hooked
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
