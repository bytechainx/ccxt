//! WebSocket transport for the pro (`watch*`) API.
//!
//! This is the Rust port of the runtime side of `ts/src/base/ws/{Client,WsClient}.ts`.
//! The transpiled `pro/<id>.rs` exchanges call — via the base `watch()` /
//! `watch_multiple()` — into a *client* object keyed by URL that:
//!   * owns the live `tokio-tungstenite` connection (one per URL),
//!   * exposes `resolve` / `reject` / `future` / `send` so the venue's
//!     `handle_message` can push parsed data back to the awaiting `watch`,
//!   * tracks `subscriptions` (so a subscribe frame is sent once per hash),
//!   * runs ping/pong keep-alive.
//!
//! Ownership model (why this is a global registry, not a field on `Exchange`):
//! the base `watch()` borrows `&mut self` to drive `handle_message`, which
//! itself needs the client. Keeping the connection + futures in a process-wide
//! registry keyed by URL (behind its own locks) keeps it disjoint from the
//! `&mut Exchange` borrow, so the drive loop can read the next frame and call
//! `handle_message(self, client, msg)` without aliasing.

#![allow(dead_code)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Instant;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use futures::{SinkExt, StreamExt};
use once_cell::sync::Lazy;
use tokio::sync::{mpsc, Notify};
use tokio_tungstenite::tungstenite::Message;

use crate::Value;

/// Per-connection state. Lives in [`REGISTRY`] behind an `Arc`, so both the
/// background reader/writer tasks and the `watch` drive loop share it.
pub struct ClientState {
    pub url: String,
    /// Frames queued for the writer task → socket.
    outgoing: mpsc::UnboundedSender<Message>,
    /// Receiver half held until the socket connects (a slot is pre-registered
    /// before connecting so `client.subscriptions` writes persist — upbit builds
    /// its subscribe frame from subscriptions set before the socket is up). The
    /// writer task takes this on connect.
    pending_rx: Mutex<Option<mpsc::UnboundedReceiver<Message>>>,
    /// Serializes the (async) connect so concurrent `ensure_client`s for a
    /// pre-registered slot don't open two sockets.
    connect_gate: tokio::sync::Mutex<()>,
    /// Optional pre-parse frame observer.
    ///
    /// Called by the reader task with the **complete frame bytes** and a
    /// monotonic timestamp sampled **before** the frame enters `incoming`.
    /// Downstream consumers use it to archive raw frames and to take number
    /// lexemes from the original text instead of the `f64`-normalised `Value`.
    ///
    /// `None` (the default) keeps behaviour byte-for-byte identical to before.
    raw_frame_hook: Mutex<Option<RawFrameHook>>,
    /// Parsed inbound messages awaiting dispatch to `handle_message`.
    ///
    /// Bounded: `incoming_capacity` frames (G4/A03). A push past the bound
    /// fails the connection closed instead of growing without limit; the drop
    /// is counted in `incoming_overflow` and reported through the event hook.
    incoming: Mutex<VecDeque<Value>>,
    /// Hard frame-count bound on `incoming`. `0` disables the bound.
    incoming_capacity: AtomicUsize,
    /// Frames rejected by the bound since this slot was created.
    incoming_overflow: AtomicU64,
    /// Monotonic counter of **physical** connections opened for this URL
    /// (G2/A02/A07). A reconnect opens a new physical socket and therefore
    /// gets a higher value than the closed slot it replaced; a reused live
    /// socket keeps the old one. `drop_client` forgets the URL and restarts
    /// the count, so a coordinator that tracks epochs must keep the slot.
    generation: AtomicU64,
    /// Optional connection-lifecycle observer (G2).
    event_hook: Mutex<Option<WsEventHook>>,
    /// Why the slot closed; `None` while open.
    close_reason: Mutex<Option<String>>,
    /// Woken on: new inbound message, a resolve/reject, or close.
    notify: Notify,
    /// messageHash → resolved value (set by `handle_message` via `resolve`).
    resolved: Mutex<HashMap<String, Value>>,
    /// messageHash → error (set by `reject`; delivered before/instead of value).
    rejections: Mutex<HashMap<String, Value>>,
    /// subscribeHash → subscription object (TS `client.subscriptions[hash]`).
    /// A subscribe frame is sent only the first time a hash is inserted.
    subscriptions: Mutex<HashMap<String, Value>>,
    /// messageHashes some `watch` call is currently waiting on. Mirrors TS
    /// `client.futures` (read by a few venues' `handle_message`).
    futures: Mutex<HashSet<String>>,
    /// In-flight single-flight slots keyed by messageHash — the registry behind
    /// TS's `client.future ()` / `client.reusableFuture ()` leader election
    /// (binance/bybit/kucoin/... `authenticate`). Separate from `resolved`
    /// because a flight has *many* waiters (all of whom must observe the
    /// settled value, not consume it) and because settling one must also clear
    /// its `futures` entry so the NEXT cycle can elect a fresh leader.
    flights: Mutex<HashMap<String, FlightSlot>>,
    connected: Mutex<bool>,
    closed: Mutex<bool>,
    last_pong_ms: Mutex<i64>,
    /// Static-WS-test mock transport. When `mock` is set, `send_text` records the
    /// (JSON-parsed) outgoing frame into `mock_sent` instead of relying on a
    /// socket, and no real connection is ever opened. `ws_test_completed` is the
    /// watch side's done-flag the frame injector's rejection loop polls.
    mock: Mutex<bool>,
    mock_sent: Mutex<Vec<Value>>,
    ws_test_completed: Mutex<bool>,
}

/// One single-flight slot. `settled` stays populated after the flight closes so
/// waiters that poll late still observe the result, and so a caller that
/// re-awaits a hash nobody is working on any more gets the last value instead
/// of blocking (TS's already-resolved Future). `open` is what a concurrent
/// caller joins.
#[derive(Clone)]
struct FlightSlot {
    open: bool,
    settled: Option<Result<Value, Value>>,
}

static REGISTRY: Lazy<Mutex<HashMap<String, Arc<ClientState>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Monotonic-ish wall clock in ms. `SystemTime` is fine here (keep-alive only).
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// A complete inbound frame **before** parsing.
///
/// Held by the reader task between `read.next()` and `parse_text` /
/// `parse_binary`, which is the only place the original bytes exist.
#[derive(Debug, Clone)]
pub enum RawFrame {
    /// A WebSocket text frame.
    Text(String),
    /// A WebSocket binary frame (may be gzip/deflate encoded).
    Binary(Vec<u8>),
}

impl RawFrame {
    /// The frame bytes as they arrived on the wire (not decompressed).
    pub fn bytes(&self) -> &[u8] {
        match self {
            RawFrame::Text(t) => t.as_bytes(),
            RawFrame::Binary(b) => b,
        }
    }
}

/// Borrowed view of a frame, used on the reader hot path so that no owned
/// [`RawFrame`] is built when no observer is installed.
#[derive(Debug, Clone, Copy)]
pub enum RawFrameRef<'a> {
    /// A WebSocket text frame.
    Text(&'a str),
    /// A WebSocket binary frame.
    Binary(&'a [u8]),
}

impl<'a> RawFrameRef<'a> {
    /// The frame bytes as they arrived on the wire (not decompressed).
    pub fn bytes(&self) -> &'a [u8] {
        match self {
            RawFrameRef::Text(t) => t.as_bytes(),
            RawFrameRef::Binary(b) => b,
        }
    }

    /// Materialise an owned frame. Only called when a hook is installed.
    pub fn into_owned(self) -> RawFrame {
        match self {
            RawFrameRef::Text(t) => RawFrame::Text(t.to_string()),
            RawFrameRef::Binary(b) => RawFrame::Binary(b.to_vec()),
        }
    }
}

/// Pre-parse frame observer: receives the frame and its arrival instant.
///
/// `Instant` is sampled at the call site in the reader task, i.e. before
/// parsing and before the frame is enqueued, so it can serve as `received_at`.
pub type RawFrameHook = std::sync::Arc<dyn Fn(RawFrame, Instant) + Send + Sync>;

/// Default hard bound on the pre-dispatch inbound queue (G4/A03).
///
/// Sized for burst tolerance, not for throughput: a consumer that cannot drain
/// this many parsed frames is not keeping up, and letting the queue grow turns
/// a visible overload into an invisible memory leak.
pub const DEFAULT_INCOMING_CAPACITY: usize = 8192;

/// One connection-lifecycle event (G2/A02/A07).
///
/// `generation` identifies the **physical** connection. It increases by exactly
/// one per successful connect, so a consumer can attach every open/close to the
/// socket it belongs to instead of inferring it from wall-clock proximity.
#[derive(Debug, Clone)]
pub enum WsEvent {
    /// A socket was opened; `generation` is unique for this slot.
    Opened { generation: u64 },
    /// A socket ended (peer close, read error, or a local failure).
    Closed { generation: u64, reason: String },
    /// The inbound queue hit its hard bound; the connection was failed closed.
    Overflow { generation: u64, dropped: u64, capacity: usize },
}

/// Connection-lifecycle observer. Runs on the task that observed the event;
/// keep it cheap and non-blocking (it holds no lock, but it is on the reader
/// path for `Overflow`).
pub type WsEventHook = std::sync::Arc<dyn Fn(WsEvent) + Send + Sync>;

/// Decode a text frame into a `Value` — JSON when it parses, else a raw string
/// (some venues send bare `"pong"` etc. that `handle_message` matches on).
fn parse_text(t: &str) -> Value {
    match serde_json::from_str::<serde_json::Value>(t) {
        Ok(j) => Value::from_json(&j),
        Err(_) => Value::Str(t.to_string().into()),
    }
}

/// Decode a binary frame: try raw-inflate then gzip (the two schemes venues
/// use), fall back to the bytes as UTF-8. Then parse as text.
fn parse_binary(b: &[u8]) -> Value {
    use std::io::Read;
    // gzip
    {
        let mut d = flate2::read::GzDecoder::new(b);
        let mut s = String::new();
        if d.read_to_string(&mut s).is_ok() && !s.is_empty() {
            return parse_text(&s);
        }
    }
    // raw deflate (no zlib header)
    {
        let mut d = flate2::read::DeflateDecoder::new(b);
        let mut s = String::new();
        if d.read_to_string(&mut s).is_ok() && !s.is_empty() {
            return parse_text(&s);
        }
    }
    // Uncompressed and valid UTF-8 → parse as text/JSON (covers venues that
    // send plain-JSON frames over the binary opcode).
    if let Ok(s) = std::str::from_utf8(b) {
        return parse_text(s);
    }
    // Genuine binary payload (e.g. mexc protobuf): carry the raw bytes through
    // as the port's byte-array form so `isBinaryMessage`/`decodeProtoMsg` see them.
    crate::exchange_stubs::bytes_to_value(b)
}

/// Decode a binary frame for a client with `decompressBinary` off (mexc
/// protobuf, paradex SBE): JSON is still read as text, as some venues send it
/// over the binary opcode, everything else is carried through as raw bytes.
/// No utf-8 fallback here: a binary payload can pass for utf-8 text.
fn parse_raw_binary(b: &[u8]) -> Value {
    match serde_json::from_slice::<serde_json::Value>(b) {
        Ok(j) => Value::from_json(&j),
        Err(_) => crate::exchange_stubs::bytes_to_value(b),
    }
}

impl ClientState {
    /// Enqueue a parsed frame. Returns `false` when the frame was **not**
    /// queued because the connection is closing or the queue hit its bound.
    ///
    /// Callers on the reader path must stop reading on `false`: the slot is
    /// already failed closed, and continuing would only burn CPU on a socket
    /// nobody is allowed to observe.
    fn push_incoming(&self, v: Value) -> bool {
        if *self.closed.lock().unwrap() {
            return false;
        }
        let capacity = self.incoming_capacity.load(Ordering::SeqCst);
        let mut q = self.incoming.lock().unwrap();
        if capacity != 0 && q.len() >= capacity {
            drop(q);
            self.overflow(capacity);
            return false;
        }
        q.push_back(v);
        drop(q);
        self.notify.notify_waiters();
        true
    }

    /// Fail the connection closed because the inbound queue is full.
    ///
    /// One `Overflow` event per connection: the closed flag is set before the
    /// event fires, so later reader frames take the `closed` fast path and do
    /// not inflate the counter.
    fn overflow(&self, capacity: usize) {
        let dropped = self.incoming_overflow.fetch_add(1, Ordering::SeqCst) + 1;
        let generation = self.generation.load(Ordering::SeqCst);
        let first = self.mark_closed("incoming queue overflow");
        if first {
            self.emit(WsEvent::Overflow { generation, dropped, capacity });
        }
    }

    /// Set (or clear, with `None`) the connection-lifecycle observer.
    pub fn set_event_hook(&self, hook: Option<WsEventHook>) {
        *self.event_hook.lock().unwrap() = hook;
    }

    /// Generation of the current (or last) physical connection. `0` means this
    /// slot never opened a socket.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    /// Frames rejected by the hard bound since this slot was created.
    pub fn overflow_count(&self) -> u64 {
        self.incoming_overflow.load(Ordering::SeqCst)
    }

    /// Why the slot closed; `None` while it is open.
    pub fn close_reason(&self) -> Option<String> {
        self.close_reason.lock().unwrap().clone()
    }

    /// Install or change the hard queue bound. `0` disables it (test/debug
    /// only — production profiles must keep a finite bound).
    pub fn set_incoming_capacity(&self, capacity: usize) {
        let queued = self.incoming.lock().unwrap().len();
        // Shrinking below the current backlog is refused rather than silently
        // truncating frames that a consumer has not seen yet.
        if capacity != 0 && capacity < queued {
            return;
        }
        self.incoming_capacity.store(capacity, Ordering::SeqCst);
    }

    /// Mark the slot closed exactly once, recording the reason and emitting
    /// `Closed`. Returns `true` for the transition that actually closed it.
    fn mark_closed(&self, reason: &str) -> bool {
        let first = {
            let mut closed = self.closed.lock().unwrap();
            if *closed {
                false
            } else {
                *closed = true;
                true
            }
        };
        *self.connected.lock().unwrap() = false;
        if first {
            *self.close_reason.lock().unwrap() = Some(reason.to_string());
        }
        self.notify.notify_waiters();
        if first {
            self.emit(WsEvent::Closed {
                generation: self.generation.load(Ordering::SeqCst),
                reason: reason.to_string(),
            });
        }
        first
    }

    /// Invoke the lifecycle observer, if one is installed, outside any lock.
    fn emit(&self, event: WsEvent) {
        let hook = self.event_hook.lock().unwrap().clone();
        if let Some(hook) = hook {
            hook(event);
        }
    }

    /// Install (or clear, with `None`) the pre-parse frame observer.
    ///
    /// The hook runs synchronously on the reader task **before** parsing and
    /// before the frame is queued, so `received_at` reflects arrival rather
    /// than dequeue time. Keep the callback cheap: it is in the hot path.
    pub fn set_raw_frame_hook(&self, hook: Option<RawFrameHook>) {
        *self.raw_frame_hook.lock().unwrap() = hook;
    }

    /// Invoke the observer, if one is installed.
    ///
    /// Takes the frame **by reference** and only materialises an owned
    /// [`RawFrame`] when a hook is actually installed. With no hook this is one
    /// `Option` check and **no `String`/`Vec` clone** — the frame text is still
    /// borrowed from the reader loop, so the hot path stays allocation-free.
    ///
    /// The `Arc` is cloned and the lock released before the call, so a hook
    /// that (re)installs itself cannot deadlock.
    fn observe_raw_frame(&self, frame: RawFrameRef<'_>, received_at: Instant) {
        let hook = self.raw_frame_hook.lock().unwrap().clone();
        if let Some(hook) = hook {
            hook(frame.into_owned(), received_at);
        }
    }

    /// Await the next inbound message. `None` once the socket is closed and
    /// the backlog is drained. Uses the create-future-before-check pattern so
    /// a resolve/push racing the await is never lost.
    pub async fn next_message(&self) -> Option<Value> {
        let mock = *self.mock.lock().unwrap();
        loop {
            let notified = self.notify.notified();
            if let Some(v) = self.incoming.lock().unwrap().pop_front() {
                return Some(v);
            }
            if *self.closed.lock().unwrap() {
                return None;
            }
            if mock {
                // Static WS test: the mock queue is fed by the frame injector.
                // If nothing arrives within a short window the fixture is
                // finished (or under-feeds this watch) — stop instead of
                // blocking the drive loop forever, so the test completes.
                if tokio::time::timeout(std::time::Duration::from_millis(1500), notified).await.is_err() {
                    return None;
                }
            } else {
                notified.await;
            }
        }
    }

    /// Store a resolved value for `hash` (TS `client.resolve`). Also settles an
    /// open single-flight on the same hash: TS's `client.resolve ()` settles
    /// the future AND removes it from `client.futures`, which is what lets the
    /// next cycle elect a fresh leader. Both paths run — a hash can in
    /// principle be someone's flight and someone else's `watch`, and only the
    /// waiter that exists will read its side.
    pub fn resolve(&self, hash: &str, value: Value) {
        self.flight_settle(hash, Ok(value.clone()));
        self.resolved.lock().unwrap().insert(hash.to_string(), value);
        self.notify.notify_waiters();
    }

    /// Store an error for `hash` (TS `client.reject`), settling an open
    /// single-flight on the same hash — see `resolve`.
    pub fn reject(&self, hash: &str, err: Value) {
        self.flight_settle(hash, Err(err.clone()));
        self.rejections.lock().unwrap().insert(hash.to_string(), err);
        self.notify.notify_waiters();
    }

    /// If any of `hashes` has a resolved value or rejection, remove and return
    /// it (`Ok` for value, `Err` for rejection). Rejections take priority.
    pub fn take_settled(&self, hashes: &[String]) -> Option<Result<Value, Value>> {
        {
            let mut rj = self.rejections.lock().unwrap();
            for h in hashes {
                if let Some(e) = rj.remove(h) {
                    return Some(Err(e));
                }
            }
        }
        let mut r = self.resolved.lock().unwrap();
        for h in hashes {
            if let Some(v) = r.remove(h) {
                return Some(Ok(v));
            }
        }
        None
    }

    /// Register interest in `hashes` (TS `client.future`). Returns true if the
    /// subscribe frame still needs sending for `subscribe_hash`.
    pub fn note_futures(&self, hashes: &[String]) {
        let mut f = self.futures.lock().unwrap();
        for h in hashes {
            f.insert(h.clone());
        }
    }

    /// Open a single-flight for `hash`, or join one already in progress. `true`
    /// means this caller is the leader and must do the work; `false` means a
    /// flight is already open and the caller should wait on it.
    ///
    /// The insert into `futures` is what makes the transpiled
    /// `if (messageHash in client.futures)` follower test observable, and it
    /// happens under the same lock as the check — this is TS's documented
    /// "atomic check-and-insert".
    pub fn flight_begin(&self, hash: &str) -> bool {
        let mut fl = self.flights.lock().unwrap();
        match fl.get_mut(hash) {
            // Open flight — join it as a follower.
            Some(slot) if slot.open => false,
            // Closed: re-open, keeping the last value for anyone who re-awaits
            // without new work being done.
            Some(slot) => {
                slot.open = true;
                self.futures.lock().unwrap().insert(hash.to_string());
                true
            }
            None => {
                fl.insert(hash.to_string(), FlightSlot { open: true, settled: None });
                self.futures.lock().unwrap().insert(hash.to_string());
                true
            }
        }
    }

    /// The flight's last outcome, if it has one. Does NOT consume it: every
    /// waiter must be able to observe the same result.
    pub fn flight_peek(&self, hash: &str) -> Option<Result<Value, Value>> {
        self.flights.lock().unwrap().get(hash).and_then(|s| s.settled.clone())
    }

    /// Whether a flight for `hash` exists and is still open.
    pub fn flight_is_open(&self, hash: &str) -> bool {
        self.flights.lock().unwrap().get(hash).map(|s| s.open).unwrap_or(false)
    }

    /// Settle an open flight and wake its waiters. Returns false (and does
    /// nothing) when `hash` names no open flight, so `resolve`/`reject` can
    /// fall through to their normal watch-hash behaviour.
    pub fn flight_settle(&self, hash: &str, res: Result<Value, Value>) -> bool {
        {
            let mut fl = self.flights.lock().unwrap();
            match fl.get_mut(hash) {
                Some(slot) if slot.open => {
                    slot.open = false;
                    slot.settled = Some(res);
                }
                _ => return false,
            }
        }
        // Clear the `futures` entry as part of settling, mirroring TS: the
        // flight is over, so the next cycle must elect a fresh leader.
        self.futures.lock().unwrap().remove(hash);
        self.notify.notify_waiters();
        true
    }

    /// Record `subscribe_hash` → `subscription`; returns true the first time
    /// (so the caller sends the subscribe frame exactly once). Mirrors TS
    /// `client.subscriptions[subscribeHash] = subscription || true`.
    pub fn subscribe_once(&self, subscribe_hash: &str, subscription: Value) -> bool {
        let mut subs = self.subscriptions.lock().unwrap();
        if subs.contains_key(subscribe_hash) {
            return false;
        }
        let stored = if matches!(subscription, Value::Null) {
            Value::Bool(true)
        } else {
            subscription
        };
        subs.insert(subscribe_hash.to_string(), stored);
        true
    }

    /// Directly set a subscription entry (TS `client.subscriptions[h] = x`
    /// written from `handle_message`).
    pub fn set_subscription(&self, subscribe_hash: &str, subscription: Value) {
        self.subscriptions.lock().unwrap().insert(subscribe_hash.to_string(), subscription);
    }

    pub fn is_subscribed(&self, subscribe_hash: &str) -> bool {
        self.subscriptions.lock().unwrap().contains_key(subscribe_hash)
    }

    pub fn send_text(&self, s: String) -> bool {
        if std::env::var("CCXT_WS_DEBUG").is_ok() { eprintln!("[wssend] {}", s.chars().take(200).collect::<String>()); }
        // Static-WS-test mock transport: record the frame (JSON-parsed, like the
        // TS `client.connection.send` stub) instead of hitting a socket.
        if *self.mock.lock().unwrap() {
            self.mock_sent.lock().unwrap().push(parse_text(&s));
            return true;
        }
        self.outgoing.send(Message::Text(s)).is_ok()
    }

    /// Enable the mock transport: mark connected (so `ensure_client` never opens
    /// a socket) and route `send_text` to the capture buffer.
    pub fn mock_enable(&self) {
        *self.mock.lock().unwrap() = true;
        *self.connected.lock().unwrap() = true;
        // The mock transport is socket-less but still a connection for the
        // purpose of lifecycle observation: consumers exercise the same
        // generation/closed path in tests as on the wire.
        let generation = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
        self.emit(WsEvent::Opened { generation });
    }

    /// Close a mock connection (fixture teardown). Real sockets close through
    /// the reader task; this mirrors that transition without one.
    pub fn mock_close(&self, reason: &str) {
        self.mark_closed(reason);
    }
    /// Injected inbound frame → the same queue (and bound) the reader feeds.
    pub fn mock_inject(&self, msg: Value) {
        let _ = self.push_incoming(msg);
    }
    pub fn is_mock(&self) -> bool { *self.mock.lock().unwrap() }
    /// The captured outgoing frames as a `Value::List`.
    pub fn mock_sent_value(&self) -> Value {
        Value::Array(self.mock_sent.lock().unwrap().clone())
    }
    pub fn has_pending_futures(&self) -> bool {
        !self.futures.lock().unwrap().is_empty()
    }
    /// Whether the mock inbound queue still holds un-consumed frames.
    pub fn has_queued_messages(&self) -> bool {
        !self.incoming.lock().unwrap().is_empty()
    }
    pub fn mark_ws_test_completed(&self) { *self.ws_test_completed.lock().unwrap() = true; }
    pub fn is_ws_test_completed(&self) -> bool { *self.ws_test_completed.lock().unwrap() }
    /// Reject every pending future so a fixture whose frames don't resolve the
    /// watch fails (with a message) rather than hanging the drive loop.
    pub fn reject_pending_futures(&self, err: Value) {
        let hashes: Vec<String> = self.futures.lock().unwrap().iter().cloned().collect();
        for h in hashes { self.reject(&h, err.clone()); }
    }

    pub fn is_closed(&self) -> bool {
        *self.closed.lock().unwrap()
    }

    pub fn on_pong(&self) {
        *self.last_pong_ms.lock().unwrap() = now_ms();
    }

    /// Drop resolved/rejected/subscription/future state,
    /// e.g. after a reconnect so stale hashes don't resolve new waiters.
    /// Internal wipe only — the transpiled `client.reset(error)` goes through
    /// `reset_with_error` below, which delivers the error first.
    pub fn reset(&self) {
        self.resolved.lock().unwrap().clear();
        self.rejections.lock().unwrap().clear();
        self.subscriptions.lock().unwrap().clear();
        self.futures.lock().unwrap().clear();
        self.flights.lock().unwrap().clear();
    }

    /// TS `client.reset(error)` — reject every pending future/flight with the
    /// error so waiters fail fast instead of hanging, then drop the pending
    /// future state. Mirrors the js/php/c#/go/java/python clients: rejections
    /// are kept (they carry the error to the waiters), delivered-but-not-taken
    /// `resolved` values are kept (in TS an already-resolved future is settled
    /// — `reject` on it is a no-op — so its waiter still gets the value), and
    /// subscriptions are left to the caller — the TS `Client.reset` does not
    /// clear them either.
    pub fn reset_with_error(&self, error: Value) {
        let hashes: Vec<String> = self.futures.lock().unwrap().iter().cloned().collect();
        for h in &hashes {
            if self.resolved.lock().unwrap().contains_key(h) {
                continue;
            }
            self.reject(h, error.clone());
        }
        self.futures.lock().unwrap().clear();
    }

    /// Snapshot of `subscriptions` as a `Value::Map { hash: subscription }` —
    /// the shape the transpiled `get_value(&client, "subscriptions")` reads.
    /// Tagged with `__ws_subs_url` so that writes performed on the snapshot
    /// (`client.subscriptions[chanId] = …`, common in bitfinex/chan-id venues)
    /// route back to this live `ClientState` instead of a discarded clone.
    pub fn subscriptions_value(&self) -> Value {
        let subs = self.subscriptions.lock().unwrap();
        let mut m = indexmap::IndexMap::new();
        m.insert("__ws_subs_url".to_string(), Value::Str(self.url.clone().into()));
        for (h, sub) in subs.iter() {
            // Tag each subscription DICT with a back-reference so a field write
            // on it (`subscription['receivedSnapshot'] = true`) persists to this
            // live client — venues mutate a subscription retrieved from
            // client.subscriptions and rely on JS object identity.
            let tagged = match sub {
                Value::Dict(d) => {
                    let mut inner = (**d).clone();
                    inner.insert("__ws_sub_ref".to_string(),
                        Value::Str(format!("{}\u{1}{}", self.url, h).into()));
                    Value::Dict(std::sync::Arc::new(inner))
                }
                other => other.clone(),
            };
            m.insert(h.clone(), tagged);
        }
        Value::Map(m)
    }

    /// Set a field on a stored subscription dict (`client.subscriptions[hash]
    /// [key] = val`). Creates the entry if missing.
    pub fn set_subscription_field(&self, hash: &str, key: &str, val: Value) {
        let mut subs = self.subscriptions.lock().unwrap();
        match subs.get_mut(hash) {
            Some(Value::Dict(d)) => { std::sync::Arc::make_mut(d).insert(key.to_string(), val); }
            _ => {
                let mut inner = indexmap::IndexMap::new();
                inner.insert(key.to_string(), val);
                subs.insert(hash.to_string(), Value::Dict(std::sync::Arc::new(inner)));
            }
        }
    }

    /// Snapshot of `futures` as a `Value::Map { hash: true }`.
    pub fn futures_value(&self) -> Value {
        let f = self.futures.lock().unwrap();
        let mut m = indexmap::IndexMap::new();
        for h in f.iter() {
            // Each entry is a future *handle* carrying the url + its own hash, so
            // transpiled `client.futures[hash].resolve(x)` (bitget/cryptocom auth)
            // routes back to this ClientState — see value_resolve/value_reject.
            let mut fh = indexmap::IndexMap::new();
            fh.insert("url".to_string(), Value::Str(self.url.clone().into()));
            fh.insert("__ws_future_hash".to_string(), Value::Str(h.clone().into()));
            m.insert(h.clone(), Value::Map(fh));
        }
        Value::Map(m)
    }
}

/// Get the existing client for `url`, or `None` if not yet connected.
pub fn get_client(url: &str) -> Option<Arc<ClientState>> {
    let reg = REGISTRY.lock().unwrap();
    reg.get(url).filter(|c| !c.is_closed()).cloned()
}

/// `client.subscriptions[key] = val` written on a tagged snapshot — persist it
/// to the live client so subsequent `handle_message` snapshots see it.
pub fn value_subs_insert(url: &str, key: &str, val: Value) {
    if let Some(c) = get_client(url) { c.set_subscription(key, val); }
}

/// `delete client.subscriptions[key]` on a tagged snapshot.
pub fn value_subs_remove(url: &str, key: &str) {
    if let Some(c) = get_client(url) { c.subscriptions.lock().unwrap().remove(key); }
}

/// `client.subscriptions[hash][key] = val` written on a tagged subscription
/// dict (carrying `__ws_sub_ref` = "url\u{1}hash").
pub fn value_sub_field_write(subref: &str, key: &str, val: Value) {
    if let Some((url, hash)) = subref.split_once('\u{1}') {
        if let Some(c) = get_client(url) { c.set_subscription_field(hash, key, val); }
    }
}

/// Ensure a live connection to `url`, connecting (and spawning the reader /
/// writer / keep-alive tasks) if needed. Idempotent per URL.
/// Open an HTTP CONNECT tunnel to `proxy` and hand the raw stream to
/// tungstenite. `connect_async` has no proxy support of its own, so a venue
/// reached through `wsProxy` / `wssProxy` has to be dialed this way.
async fn connect_via_proxy(
    url: &str,
    proxy: &str,
) -> Result<tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>, String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let target = url::Url::parse(url).map_err(|e| format!("[NetworkError] ws url {url}: {e}"))?;
    let host = target.host_str().ok_or_else(|| format!("[NetworkError] ws url {url} has no host"))?;
    let port = target.port_or_known_default().unwrap_or(443);
    let p = url::Url::parse(proxy).map_err(|e| format!("[NetworkError] ws proxy {proxy}: {e}"))?;
    let p_host = p.host_str().ok_or_else(|| format!("[NetworkError] ws proxy {proxy} has no host"))?;
    let p_port = p.port_or_known_default().unwrap_or(8080);
    let mut stream = tokio::net::TcpStream::connect((p_host, p_port))
        .await
        .map_err(|e| format!("[NetworkError] ws proxy connect {proxy}: {e}"))?;
    let connect = format!(
        "CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\nProxy-Connection: Keep-Alive\r\n\r\n"
    );
    stream
        .write_all(connect.as_bytes())
        .await
        .map_err(|e| format!("[NetworkError] ws proxy CONNECT {proxy}: {e}"))?;
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        let n = stream
            .read(&mut byte)
            .await
            .map_err(|e| format!("[NetworkError] ws proxy CONNECT {proxy}: {e}"))?;
        if n == 0 {
            return Err(format!("[NetworkError] ws proxy {proxy} closed during CONNECT"));
        }
        head.push(byte[0]);
        if head.len() > 8192 {
            return Err(format!("[NetworkError] ws proxy {proxy} sent an oversized CONNECT reply"));
        }
    }
    let status = String::from_utf8_lossy(&head);
    let first = status.lines().next().unwrap_or_default();
    if !first.contains(" 200") {
        return Err(format!("[NetworkError] ws proxy {proxy} refused CONNECT: {first}"));
    }
    let (ws, _resp) = tokio_tungstenite::client_async_tls(url, stream)
        .await
        .map_err(|e| format!("[NetworkError] ws connect {url} via {proxy}: {e}"))?;
    Ok(ws)
}

pub async fn ensure_client(url: &str, proxy: Option<String>, decompress_binary: bool) -> Result<Arc<ClientState>, String> {
    ensure_client_with_hook(url, proxy, decompress_binary, None).await
}

/// Like [`ensure_client`], but installs a pre-parse frame observer **before the
/// socket is opened**.
///
/// Use this when the first frame matters. With plain [`ensure_client`] the
/// caller can only install the hook after the connection is already up, so any
/// frame that arrives in the meantime is lost — the observer is a live tap, not
/// a replay buffer.
///
/// The hook is applied when the slot is created, which happens before the reader
/// task is spawned. If a slot for `url` already exists and is still open, it is
/// reused and the hook is installed on it: that still covers frames from this
/// point on, but it cannot recover frames that arrived before the call (another
/// consumer may already own the socket — slots are keyed by URL and shared).
pub async fn ensure_client_with_hook(
    url: &str,
    proxy: Option<String>,
    decompress_binary: bool,
    hook: Option<RawFrameHook>,
) -> Result<Arc<ClientState>, String> {
    ensure_client_with_hooks(url, proxy, decompress_binary, hook, None).await
}

/// Like [`ensure_client_with_hook`], but also installs the connection-lifecycle
/// observer **before** the socket opens, so the `Opened` event of the first
/// connection is not missed (G2).
pub async fn ensure_client_with_hooks(
    url: &str,
    proxy: Option<String>,
    decompress_binary: bool,
    hook: Option<RawFrameHook>,
    events: Option<WsEventHook>,
) -> Result<Arc<ClientState>, String> {
    let state = ensure_slot_with_hook(url, hook);
    if let Some(events) = events {
        state.set_event_hook(Some(events));
    }
    if *state.connected.lock().unwrap() {
        return Ok(state);
    }
    // Serialize connect; re-check under the gate (another task may have just
    // connected this slot). Lock via a cloned Arc so `state` stays movable.
    let gate_holder = state.clone();
    let _gate = gate_holder.connect_gate.lock().await;
    if *state.connected.lock().unwrap() {
        return Ok(state);
    }
    let ws = match proxy.as_deref().filter(|p| !p.is_empty()) {
        Some(p) => connect_via_proxy(url, p).await?,
        None => {
            let (ws, _resp) = tokio_tungstenite::connect_async(url)
                .await
                .map_err(|e| format!("[NetworkError] ws connect {url}: {e}"))?;
            ws
        }
    };
    let (mut write, mut read) = ws.split();
    let mut rx = state.pending_rx.lock().unwrap().take()
        .ok_or_else(|| format!("[NetworkError] ws {url} slot has no writer channel"))?;
    *state.last_pong_ms.lock().unwrap() = now_ms();

    // Writer task: drain the outgoing queue to the socket.
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if write.send(msg).await.is_err() {
                break;
            }
        }
        let _ = write.close().await;
    });

    // Reader task: decode frames → incoming queue.
    let st = state.clone();
    tokio::spawn(async move {
        while let Some(frame) = read.next().await {
            match frame {
                Ok(Message::Text(t)) => {
                    // Sample arrival time BEFORE parsing/queueing (G1).
                    // Borrowed view: no clone unless a hook is installed.
                    st.observe_raw_frame(RawFrameRef::Text(&t), Instant::now());
                    if !st.push_incoming(parse_text(&t)) {
                        break;
                    }
                }
                Ok(Message::Binary(b)) => {
                    st.observe_raw_frame(RawFrameRef::Binary(&b), Instant::now());
                    if !st.push_incoming(if decompress_binary { parse_binary(&b) } else { parse_raw_binary(&b) }) {
                        break;
                    }
                }
                Ok(Message::Pong(_)) => st.on_pong(),
                // tungstenite answers Ping frames with Pong automatically.
                Ok(Message::Ping(_)) => {}
                Ok(Message::Close(_)) | Err(_) => break,
                _ => {}
            }
        }
        // One close transition per connection: a reader that stopped because
        // of an overflow must not overwrite the reason recorded there.
        st.mark_closed("socket closed");
    });

    // Keep-alive: an unsolicited Ping every 30s; the peer's Pong updates
    // last_pong. (Full miss-count RequestTimeout handling is a later refinement.)
    let st2 = state.clone();
    tokio::spawn(async move {
        let mut iv = tokio::time::interval(std::time::Duration::from_secs(30));
        iv.tick().await; // first tick fires immediately; skip it
        loop {
            iv.tick().await;
            if st2.is_closed() {
                break;
            }
            if st2.outgoing.send(Message::Ping(Vec::new())).is_err() {
                break;
            }
        }
    });

    // G2: the socket is up. Publish its generation *before* the reader can
    // dispatch anything, so every frame and close is attributable to exactly
    // one physical connection.
    let generation = state.generation.fetch_add(1, Ordering::SeqCst) + 1;
    *state.connected.lock().unwrap() = true;
    state.emit(WsEvent::Opened { generation });
    Ok(state)
}

/// Live read of a client handle's `subscriptions` / `futures` field, straight
/// from the registry so a read after a write (upbit builds its subscribe frame
/// from subscriptions it just set) is coherent, not a stale embedded snapshot.
pub fn client_field_live(url: &str, field: &str) -> Value {
    match get_client(url) {
        Some(c) if field == "futures" => c.futures_value(),
        Some(c) => c.subscriptions_value(),
        None => Value::Map(indexmap::IndexMap::new()),
    }
}

/// Get-or-create the registry slot for `url` WITHOUT connecting the socket, so
/// `client.subscriptions` written before `watch()` connects still persist.
fn ensure_slot(url: &str) -> Arc<ClientState> {
    ensure_slot_with_hook(url, None)
}

/// Like [`ensure_slot`], but installs `hook` on a **newly created** slot before it
/// is returned.
///
/// Installing here (rather than after `ensure_client` returns) is what makes the
/// observer safe for the first frame: the reader task is only spawned later,
/// inside `ensure_client`, so a hook installed at slot creation is guaranteed to
/// be in place before any frame can arrive.
///
/// If the slot already exists it is reused as-is; `hook`, when provided, is
/// installed on it (see the note on [`ensure_client_with_hook`]).
fn ensure_slot_with_hook(url: &str, hook: Option<RawFrameHook>) -> Arc<ClientState> {
    let mut reg = REGISTRY.lock().unwrap();
    // A reconnect after a close creates a fresh slot; carry the generation
    // forward so epochs stay monotonic for the URL instead of restarting at 1
    // and making two physical connections indistinguishable (G2/A02).
    let mut previous_generation = 0;
    if let Some(c) = reg.get(url) {
        if c.is_closed() {
            previous_generation = c.generation();
        }
        if !c.is_closed() {
            let existing = c.clone();
            drop(reg);
            if let Some(hook) = hook {
                existing.set_raw_frame_hook(Some(hook));
            }
            return existing;
        }
    }
    let (tx, rx) = mpsc::unbounded_channel::<Message>();
    let state = Arc::new(ClientState {
        url: url.to_string(),
        outgoing: tx,
        pending_rx: Mutex::new(Some(rx)),
        connect_gate: tokio::sync::Mutex::new(()),
        raw_frame_hook: Mutex::new(hook),
        incoming: Mutex::new(VecDeque::new()),
        incoming_capacity: AtomicUsize::new(DEFAULT_INCOMING_CAPACITY),
        incoming_overflow: AtomicU64::new(0),
        generation: AtomicU64::new(previous_generation),
        event_hook: Mutex::new(None),
        close_reason: Mutex::new(None),
        notify: Notify::new(),
        resolved: Mutex::new(HashMap::new()),
        rejections: Mutex::new(HashMap::new()),
        subscriptions: Mutex::new(HashMap::new()),
        futures: Mutex::new(HashSet::new()),
        flights: Mutex::new(HashMap::new()),
        connected: Mutex::new(false),
        closed: Mutex::new(false),
        last_pong_ms: Mutex::new(now_ms()),
        mock: Mutex::new(false),
        mock_sent: Mutex::new(Vec::new()),
        ws_test_completed: Mutex::new(false),
    });
    reg.insert(url.to_string(), state.clone());
    state
}

/// Remove (and thereby drop / disconnect) the client for `url`.
pub fn drop_client(url: &str) {
    REGISTRY.lock().unwrap().remove(url);
}

// ── `Value`-handle bridge ────────────────────────────────────────────────────
//
// The transpiled `handle_message(&mut self, client: Value, message: Value)`
// receives a *client* as a `Value`. We model it as `Value::Map { "url": <url>,
// "subscriptions": <snapshot>, "futures": <snapshot> }`. The `Value` methods
// `resolve`/`reject`/`send`/… (in value.rs) extract `url` and route here.

// ── Static-WS-test mock transport (url-keyed façade over ClientState) ────────

/// `setupWsMockTransport(url)` — register a connected, socket-less client whose
/// sends are captured. Clears any prior state so each fixture starts fresh.
pub fn mock_setup(url: &str) {
    let c = ensure_slot(url);
    c.reset();
    c.mock_enable();
    c.mock_sent.lock().unwrap().clear();
    // Discard any messages left unconsumed by a prior test on the same URL (e.g.
    // a heartbeat a watchTrades drive loop returned past before reading) so they
    // don't leak an extra handler dispatch / sent frame into the next test.
    c.incoming.lock().unwrap().clear();
    *c.ws_test_completed.lock().unwrap() = false;
}
pub fn mock_inject(url: &str, msg: Value) {
    if let Some(c) = get_client(url) { c.mock_inject(msg); }
}
pub fn mock_sent_messages(url: &str) -> Value {
    match get_client(url) { Some(c) => c.mock_sent_value(), None => Value::Array(vec![]) }
}
pub fn mock_has_pending_futures(url: &str) -> bool {
    get_client(url).map(|c| c.has_pending_futures()).unwrap_or(false)
}
pub fn mock_has_queued_messages(url: &str) -> bool {
    get_client(url).map(|c| c.has_queued_messages()).unwrap_or(false)
}
pub fn mock_mark_completed(url: &str) {
    if let Some(c) = get_client(url) { c.mark_ws_test_completed(); }
}
pub fn mock_is_completed(url: &str) -> bool {
    get_client(url).map(|c| c.is_ws_test_completed()).unwrap_or(true)
}
pub fn mock_reject_futures(url: &str) {
    if let Some(c) = get_client(url) {
        c.reject_pending_futures(Value::Str(
            "[ExchangeError] static ws test: the injected messages did not resolve the watch future".to_string().into()));
    }
}

/// Build the client-handle `Value` passed to `handle_message`: the URL plus
/// live snapshots of `subscriptions` / `futures` (the fields venues read via
/// `get_value(&client, "subscriptions")`).
pub fn client_value(url: &str) -> Value {
    // Pre-register the slot so subscriptions written on this handle (before the
    // socket connects) persist and read back — upbit-style subscribe building.
    let c = ensure_slot(url);
    let mut m = indexmap::IndexMap::new();
    m.insert("url".to_string(), Value::Str(url.to_string().into()));
    m.insert("subscriptions".to_string(), c.subscriptions_value());
    m.insert("futures".to_string(), c.futures_value());
    Value::Map(m)
}

/// Open-or-join the single-flight for `hash` on `url`, creating the registry
/// slot if the client has not been dialed yet (venue `authenticate` parks its
/// flights on a never-dialed pseudo-url, because the real user-data url embeds
/// the credential the flight is fetching).
pub fn begin_flight(url: &str, hash: &str) -> bool {
    ensure_slot(url).flight_begin(hash)
}


/// The `Value` a `client.future ()` / `client.reusableFuture ()` call hands
/// back: enough to find the flight again (its client's url + the hash). TS
/// returns a real Future object; a `Value` can't hold one, so the port returns
/// a handle and `ws_await_flight` does the awaiting.
pub fn flight_handle(url: &str, hash: &str, led: bool) -> Value {
    let mut m = indexmap::IndexMap::new();
    m.insert("url".to_string(), Value::Str(url.to_string().into()));
    m.insert("__ws_flight_hash".to_string(), Value::Str(hash.to_string().into()));
    // Whether the call that produced this handle opened the flight. A leader
    // settles its own flight (and TS's `await future` resolves the instant it
    // does), so it must never block on itself — including when the venue
    // decides, after taking the lead, that there is no work to do because it
    // is already subscribed (bitget's `authenticate`).
    m.insert("__ws_flight_led".to_string(), Value::Bool(led));
    Value::Map(m)
}

/// The hash a flight handle refers to. Accepts the `futures_value ()` handle
/// shape too, so `client.futures[hash]` entries can be awaited as well.
fn flight_hash_of(handle: &Value) -> Option<String> {
    for key in ["__ws_flight_hash", "__ws_future_hash"] {
        if let Value::Str(h) = crate::get_value(handle, &Value::Str(key.to_string().into())) {
            return Some(h.to_string());
        }
    }
    None
}

/// Await a flight handle — the port of TS's `await client.future (hash)` and
/// of the trailing `await future` a single-flight leader runs after settling.
///
/// Returns the resolved value; a rejected flight panics with the error, which
/// is how the port rethrows (the drive loop does the same with `take_settled`).
/// A handle that names no flight returns `Value::Null` rather than hanging.
pub async fn ws_await_flight(handle: &Value) -> Value {
    let (url, hash) = match (url_of(handle), flight_hash_of(handle)) {
        (Some(u), Some(h)) => (u, h),
        _ => return Value::Null,
    };
    let client = match get_client(&url) {
        Some(c) => c,
        None => return Value::Null,
    };
    let settled = client.flight_peek(&hash);
    // The leader: its own `client.resolve ()` has already run by the time it
    // reaches its trailing await, so take the value and go. Waiting here would
    // deadlock the (common) case where the venue took the lead and then found
    // it had nothing to do.
    let led = matches!(
        crate::get_value(handle, &Value::Str("__ws_flight_led".into())),
        Value::Bool(true)
    );
    if led || !client.flight_is_open(&hash) {
        return match settled {
            Some(Ok(v)) => v,
            Some(Err(e)) => panic!(
                "{}",
                match &e {
                    Value::Str(s) => s.clone(),
                    v => crate::runtime::stringify_param(v).into(),
                }
            ),
            // A flight that never opened is not an error — the value it would
            // have carried is already in the venue's own cache, which is what
            // the caller reads next.
            None => Value::Null,
        };
    }
    let budget = if client.is_mock() {
        std::time::Duration::from_millis(1500)
    } else {
        std::time::Duration::from_secs(60)
    };
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        // Create the wakeup before peeking, so a settle racing this loop is
        // never missed (same pattern as `next_message`).
        let notified = client.notify.notified();
        if let Some(res) = client.flight_peek(&hash) {
            return match res {
                Ok(v) => v,
                Err(e) => panic!(
                    "{}",
                    match &e {
                        Value::Str(s) => s.clone(),
                        v => crate::runtime::stringify_param(v).into(),
                    }
                ),
            };
        }
        if !client.flight_is_open(&hash) {
            // Closed without a value (reset between fixture cases).
            return Value::Null;
        }
        if tokio::time::timeout_at(deadline, notified).await.is_err() {
            panic!(
                "[NetworkError] timed out waiting for the in-flight '{}' on {}",
                hash, url
            );
        }
    }
}

/// Extract the `url` from a client-handle `Value` (`Map{"url": ...}`).
pub fn url_of(client: &Value) -> Option<String> {
    match crate::get_value(client, &Value::Str("url".into())) {
        Value::Str(s) => Some(s.to_string()),
        _ => None,
    }
}

fn hash_str(v: &Value) -> Option<String> {
    match v {
        Value::Str(s) => Some(s.to_string()),
        _ => None,
    }
}

/// `client.resolve(value, messageHash)` routed by URL. Returns `value`.
pub fn value_resolve(client: &Value, args: &[Value]) -> Value {
    let value = args.get(0).cloned().unwrap_or(Value::Null);
    // `client.futures[hash].resolve(value)` — the future handle carries its own
    // hash, so there is no second arg. Resolve that hash.
    if let Some(hash) = future_hash_of(client) {
        if let Some(url) = url_of(client) {
            if let Some(c) = get_client(&url) { c.resolve(&hash, value.clone()); }
        }
        return value;
    }
    if let (Some(url), Some(hash)) = (url_of(client), args.get(1).and_then(hash_str)) {
        if let Some(c) = get_client(&url) {
            c.resolve(&hash, value.clone());
        }
    }
    value
}

/// Extract the hash a future handle (`client.futures[hash]`) resolves/rejects.
fn future_hash_of(client: &Value) -> Option<String> {
    match crate::get_value(client, &Value::Str("__ws_future_hash".into())) {
        Value::Str(s) => Some(s.to_string()),
        _ => None,
    }
}

/// `client.reject(error, messageHash)` routed by URL.
pub fn value_reject(client: &Value, args: &[Value]) -> Value {
    let err = args.get(0).cloned().unwrap_or(Value::Null);
    // `client.futures[hash].reject(error)` — future handle carries its own hash.
    if let Some(hash) = future_hash_of(client) {
        if let Some(url) = url_of(client) {
            if let Some(c) = get_client(&url) { c.reject(&hash, err.clone()); }
        }
        return err;
    }
    if let Some(url) = url_of(client) {
        if let Some(c) = get_client(&url) {
            match args.get(1).and_then(hash_str) {
                Some(hash) => c.reject(&hash, err.clone()),
                // reject with no hash → reject every pending future.
                None => {
                    let hashes: Vec<String> = c.futures.lock().unwrap().iter().cloned().collect();
                    for h in hashes {
                        c.reject(&h, err.clone());
                    }
                }
            }
        }
    }
    err
}

/// `client.send(message)` routed by URL. Serialises non-string payloads to JSON.
pub fn value_send(client: &Value, args: &[Value]) -> Value {
    if let Some(url) = url_of(client) {
        if let Some(c) = get_client(&url) {
            let payload = match args.get(0) {
                Some(Value::Str(s)) => s.to_string(),
                Some(v) => v.to_json().to_string(),
                None => String::new().into(),
            };
            c.send_text(payload);
        }
    }
    Value::Null
}

/// `client.reset(...)` routed by URL. An arg-less `client.reset()` reaches
/// here with `Value::Null` — there is no error to deliver, so fall back to
/// the internal wipe rather than rejecting every waiter with a null error.
pub fn value_reset(client: &Value, error: Value) -> Value {
    if let Some(url) = url_of(client) {
        if let Some(c) = get_client(&url) {
            if matches!(error, Value::Null) {
                c.reset();
            } else {
                c.reset_with_error(error);
            }
        }
    }
    Value::Null
}

/// `client.on_pong(...)` routed by URL.
pub fn value_on_pong(client: &Value) -> Value {
    if let Some(url) = url_of(client) {
        if let Some(c) = get_client(&url) {
            c.on_pong();
        }
    }
    Value::Null
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_tungstenite::tungstenite::Message as WsMessage;

    // A minimal mock exchange WS server: accepts one connection, waits for a
    // subscribe frame, then streams a few JSON "ticker" messages. Proves the
    // full connect → send(subscribe) → receive → parse → resolve round-trip
    // without touching a live venue.
    async fn spawn_mock_server() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                // Wait for the client's subscribe frame.
                if let Some(Ok(WsMessage::Text(sub))) = ws.next().await {
                    assert!(sub.contains("subscribe"), "expected subscribe, got {sub}");
                }
                // Stream three ticker updates.
                for px in ["100.5", "101.0", "101.5"] {
                    let msg = format!("{{\"channel\":\"ticker\",\"symbol\":\"BTC/USDT\",\"last\":\"{px}\"}}");
                    ws.send(WsMessage::Text(msg)).await.unwrap();
                }
                // Give the client time to drain before closing.
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                let _ = ws.close(None).await;
            }
        });
        format!("ws://{addr}")
    }

    // The transpiled `client.reset(error)` path (`value_reset` →
    // `reset_with_error`): a pending watch hash must observe the error (not
    // hang), a delivered-but-not-taken value must survive the reset, and an
    // arg-less `client.reset()` (error = `Value::Null`) must fall back to the
    // silent wipe instead of rejecting waiters with a null error.
    #[test]
    fn value_reset_rejects_pending_futures() {
        let url = "ws://reset-with-error.test";
        mock_setup(url);
        let client = get_client(url).expect("mock client");
        client.note_futures(&["pending".to_string(), "delivered".to_string()]);
        client.resolve("delivered", Value::Str("payload".into()));

        let handle = client_value(url);
        let error = Value::Str("[InvalidNonce] gap detected".into());
        value_reset(&handle, error.clone());

        // The pending waiter fails fast with the reset error…
        assert_eq!(
            client.take_settled(&["pending".to_string()]),
            Some(Err(error))
        );
        // …while the already-delivered value still reaches its waiter.
        assert_eq!(
            client.take_settled(&["delivered".to_string()]),
            Some(Ok(Value::Str("payload".into())))
        );
        assert!(!client.has_pending_futures());

        // Arg-less reset: silent wipe, nothing settled with a null error.
        client.note_futures(&["pending2".to_string()]);
        value_reset(&handle, Value::Null);
        assert_eq!(client.take_settled(&["pending2".to_string()]), None);
        assert!(!client.has_pending_futures());

        drop_client(url);
    }

    #[tokio::test]
    async fn transport_roundtrip() {
        let url = spawn_mock_server().await;
        let client = ensure_client(&url, None, true).await.expect("connect");

        // Send a subscribe frame once (idempotent per hash).
        assert!(client.subscribe_once("ticker:BTC/USDT", Value::Null));
        assert!(!client.subscribe_once("ticker:BTC/USDT", Value::Null));
        assert!(client.send_text("{\"op\":\"subscribe\",\"channel\":\"ticker\"}".to_string()));

        // Drive: pull each inbound message, mimic handle_message resolving the
        // "ticker" hash, and collect the resolved values.
        let mut lasts = Vec::new();
        while lasts.len() < 3 {
            let msg = client.next_message().await.expect("message before close");
            let last = crate::get_value(&msg, &Value::Str("last".into()));
            if let Value::Str(s) = &last {
                client.resolve("ticker", Value::Str(s.clone()));
            }
            if let Some(Ok(Value::Str(v))) = client.take_settled(&["ticker".to_string()]) {
                lasts.push(v);
            }
        }
        assert_eq!(lasts, vec!["100.5", "101.0", "101.5"]);

        // Field snapshots the transpiled code reads off the client handle.
        let subs = client.subscriptions_value();
        assert!(crate::runtime::is_true(&crate::get_value(
            &subs,
            &Value::Str("ticker:BTC/USDT".into())
        )));

        drop_client(&url);
    }

    // A minimal Core whose `handle_message` resolves the "ticker" hash with the
    // inbound message's `last` field — exactly what a real venue's
    // handle_message does. Lets us drive the *actual* `watch()` runtime end to
    // end (connect → subscribe → frame → dispatch_to_derived("handle_message")
    // → resolve → return) against the mock server, independent of venue quirks.
    struct TestWsCore {
        exchange: crate::exchange::Exchange,
    }
    impl std::ops::Deref for TestWsCore {
        type Target = crate::exchange::Exchange;
        fn deref(&self) -> &Self::Target {
            &self.exchange
        }
    }
    impl std::ops::DerefMut for TestWsCore {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.exchange
        }
    }
    impl crate::exchange::DerivedExchange for TestWsCore {}
    impl crate::exchange_generated::ExchangeBase for TestWsCore {
        fn call_dynamic<'a>(
            &'a mut self,
            method: &'a str,
            args: Vec<Value>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Value> + Send + 'a>> {
            Box::pin(async move {
                match method {
                    "handle_message" => {
                        let client = args.get(0).cloned().unwrap_or(Value::Null);
                        let message = args.get(1).cloned().unwrap_or(Value::Null);
                        let last = crate::get_value(&message, &Value::Str("last".into()));
                        // `client.resolve(value, messageHash)` — routes to the registry.
                        client.resolve(&[last, Value::Str("ticker".into())]);
                        Value::Null
                    }
                    _ => self.call_dynamic_base(method, args).await,
                }
            })
        }
    }

    #[tokio::test]
    async fn watch_drive_loop_end_to_end() {
        use crate::exchange::ExchangeRuntime;
        let url = spawn_mock_server().await;
        let mut core = TestWsCore {
            exchange: crate::exchange::Exchange::new(None),
        };
        // Drive the real base `watch()`: connect, send subscribe, read frames,
        // dispatch each to handle_message, return once "ticker" resolves.
        let result = ExchangeRuntime::watch(
            &mut core,
            Value::Str(url.clone().into()),
            Value::Str("ticker".into()),
            &[
                Value::Str("{\"op\":\"subscribe\",\"channel\":\"ticker\"}".into()),
                Value::Str("ticker".into()),
                Value::Null,
            ],
        )
        .await;
        // First streamed ticker.
        assert_eq!(result, Value::Str("100.5".into()));
        drop_client(&url);
    }

    // ── single-flight (client.future / client.reusableFuture) ───────────────
    //
    // The venue `authenticate` idiom: the first caller leads and fetches the
    // credential, concurrent callers join the flight and wake when it settles.
    // Regression cover for the port's original no-op `future()` stub, where
    // every caller led and nobody waited.

    #[tokio::test]
    async fn flight_elects_one_leader_and_registers_it() {
        let url = "flight-test-election";
        let c = ensure_slot(url);
        assert!(c.flight_begin("authenticate:future"), "first caller leads");
        assert!(!c.flight_begin("authenticate:future"), "second caller follows");
        // The follower's `messageHash in client.futures` test must see it.
        assert!(in_op_futures(&c, "authenticate:future"));
        drop_client(url);
    }

    #[tokio::test]
    async fn flight_settle_wakes_waiters_and_reopens() {
        let url = "flight-test-settle";
        let c = ensure_slot(url);
        assert!(c.flight_begin("auth"));
        // Every waiter observes the value — settling must not consume it.
        c.resolve("auth", Value::Str("listen-key".into()));
        assert_eq!(
            ws_await_flight(&flight_handle(url, "auth", false)).await,
            Value::Str("listen-key".into())
        );
        assert_eq!(
            ws_await_flight(&flight_handle(url, "auth", false)).await,
            Value::Str("listen-key".into())
        );
        // Settled ⇒ cleared from `futures`, so the next cycle re-leads rather
        // than parking every caller on the follower branch forever.
        assert!(!in_op_futures(&c, "auth"));
        assert!(c.flight_begin("auth"), "a settled flight re-elects");
        drop_client(url);
    }

    #[tokio::test]
    async fn flight_follower_waits_for_the_leader() {
        let url = "flight-test-wait";
        let c = ensure_slot(url);
        assert!(c.flight_begin("auth"));
        let settler = {
            let c = c.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(60)).await;
                c.resolve("auth", Value::Str("late".into()));
            })
        };
        let started = std::time::Instant::now();
        let got = ws_await_flight(&flight_handle(url, "auth", false)).await;
        assert_eq!(got, Value::Str("late".into()));
        assert!(started.elapsed() >= std::time::Duration::from_millis(50), "waited for the leader");
        settler.await.unwrap();
        drop_client(url);
    }

    #[tokio::test]
    async fn a_leader_never_blocks_on_its_own_flight() {
        // A venue can take the lead and then find it has nothing to do — bitget
        // re-`authenticate`s on an already-subscribed client. Its trailing
        // `await future` must return, not wait for a settle that never comes.
        let url = "flight-test-leader";
        let c = ensure_slot(url);
        let led = c.flight_begin("authenticated");
        assert!(led);
        let started = std::time::Instant::now();
        assert_eq!(
            ws_await_flight(&flight_handle(url, "authenticated", led)).await,
            Value::Null
        );
        assert!(started.elapsed() < std::time::Duration::from_millis(200), "returned at once");
        // Having led once and settled, the value is still there for a re-await.
        c.resolve("authenticated", Value::Bool(true));
        let led2 = c.flight_begin("authenticated");
        assert_eq!(
            ws_await_flight(&flight_handle(url, "authenticated", led2)).await,
            Value::Bool(true),
            "a re-lead with no new work reports the last value"
        );
        drop_client(url);
    }

    #[tokio::test]
    async fn flight_handle_without_a_flight_returns_null() {
        // Never-opened flight: return rather than hang, the value the caller
        // wanted is already in the venue's own cache.
        let url = "flight-test-missing";
        ensure_slot(url);
        assert_eq!(ws_await_flight(&flight_handle(url, "nope", false)).await, Value::Null);
        drop_client(url);
    }

    /// The transpiled follower test, `messageHash in client.futures`.
    fn in_op_futures(c: &Arc<ClientState>, hash: &str) -> bool {
        !matches!(
            crate::get_value(&c.futures_value(), &Value::Str(hash.to_string().into())),
            Value::Null
        )
    }

    #[tokio::test]
    async fn parse_helpers() {
        // JSON text → dict; non-JSON → Value::Str.
        assert!(matches!(parse_text("{\"a\":1}"), Value::Dict(_)));
        assert_eq!(parse_text("pong"), Value::Str("pong".into()));
        // gzip binary → parsed JSON.
        use std::io::Write;
        let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        e.write_all(b"{\"x\":true}").unwrap();
        let gz = e.finish().unwrap();
        assert!(matches!(parse_binary(&gz), Value::Dict(_)));
    }

    #[test]
    fn raw_binary_frames_stay_bytes() {
        // an SBE heartbeat header is all small integers, so the frame is valid
        // utf-8, it must still reach the exchange as bytes with decompressBinary off
        let frame: Vec<u8> = vec![0x10, 0x00, 0x28, 0x00, 0x01, 0x00, 0x02, 0x00, 0x41, 0x42, 0x43, 0x01];
        assert!(std::str::from_utf8(&frame).is_ok());
        assert_eq!(parse_raw_binary(&frame), crate::exchange_stubs::bytes_to_value(&frame));
        // json over the binary opcode is still parsed
        assert!(matches!(parse_raw_binary(b"{\"x\":true}"), Value::Dict(_)));
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;

    /// G4/A03: the first queue is hard-bounded and an overflow fails the
    /// connection closed exactly once, with an observable count and event.
    #[test]
    fn bounded_queue_fails_closed_on_overflow() {
        let url = "wss://lifecycle.test/g4-overflow";
        let c = ensure_slot(url);
        c.set_incoming_capacity(2);
        let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        c.set_event_hook(Some(Arc::new(move |event| {
            sink.lock().unwrap().push(match event {
                WsEvent::Opened { generation } => format!("opened:{generation}"),
                WsEvent::Closed { generation, reason } => format!("closed:{generation}:{reason}"),
                WsEvent::Overflow { generation, dropped, capacity } => {
                    format!("overflow:{generation}:{dropped}:{capacity}")
                }
            });
        })));

        assert!(c.push_incoming(Value::Int(1)));
        assert!(c.push_incoming(Value::Int(2)));
        assert!(!c.push_incoming(Value::Int(3)));
        assert_eq!(c.overflow_count(), 1);
        assert!(c.is_closed());
        assert_eq!(c.close_reason().as_deref(), Some("incoming queue overflow"));

        // Frames after the close are dropped without inflating the counter or
        // re-emitting the overflow event.
        assert!(!c.push_incoming(Value::Int(4)));
        assert_eq!(c.overflow_count(), 1);

        let seen = seen.lock().unwrap().clone();
        assert_eq!(seen.iter().filter(|s| s.starts_with("overflow:")).count(), 1, "{seen:?}");
        assert_eq!(seen.iter().filter(|s| s.starts_with("closed:")).count(), 1, "{seen:?}");
        assert_eq!(c.incoming.lock().unwrap().len(), 2);
        drop_client(url);
    }

    /// G2/A02: a connection carries a generation and a close transition that a
    /// consumer can observe and attribute.
    #[test]
    fn connection_generation_and_close_are_observable() {
        let url = "wss://lifecycle.test/g2-epoch";
        mock_setup(url);
        let c = get_client(url).expect("mock slot");
        assert_eq!(c.generation(), 1);
        assert!(!c.is_closed());
        let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        c.set_event_hook(Some(Arc::new(move |event| {
            sink.lock().unwrap().push(match event {
                WsEvent::Opened { generation } => format!("opened:{generation}"),
                WsEvent::Closed { generation, reason } => format!("closed:{generation}:{reason}"),
                WsEvent::Overflow { .. } => "overflow".to_string(),
            });
        })));
        c.mock_close("peer closed");
        assert!(c.is_closed());
        assert_eq!(c.close_reason().as_deref(), Some("peer closed"));
        assert_eq!(seen.lock().unwrap().as_slice(), &["closed:1:peer closed".to_string()]);
        drop_client(url);
    }

    /// G2/A02: a physical reconnect after a close must land on a **higher**
    /// epoch, not restart at 1.
    #[test]
    fn reconnect_keeps_the_generation_monotonic() {
        let url = "wss://lifecycle.test/g2-reconnect";
        mock_setup(url);
        let first = get_client(url).expect("first slot");
        assert_eq!(first.generation(), 1);
        first.mock_close("peer closed");
        assert!(first.is_closed());

        mock_setup(url);
        let second = get_client(url).expect("second slot");
        assert_eq!(second.generation(), 2);
        drop_client(url);
    }
}
