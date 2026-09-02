# go-moex — handoff

Living document. Updated after every significant task (architecture change,
module addition, refactor) per project convention — see repository root
`AGENTS`/chat history. Read this before touching FORTS internals; most
"why is this weird" questions are answered here rather than re-litigated
in code comments.

## 1. Project scope and staging

- **v1.0 (current)** — FORTS (Derivatives market: futures + options,
  `engine=futures&market=forts` on ISS) MVP: reference data + historical
  candles (ISS), order entry (FIX Gate), market data (SIMBA SPECTRA).
- **v2.0** — Shares (equities, T+ mode) as an independent `shares/`
  package on the same internal transport layer (internal/iss,
  internal/fix, internal/simba, orderbook) — never by reusing `forts/*`
  code (see architecture principle below).
- **v2.5** — full exchange coverage (FX market, Money market, remaining
  ISS-only instruments).

## 2. Architecture principles (do not violate)

1. **Two layers, no cross-section reuse.** `internal/*` (iss, fix, simba)
   and `orderbook` are protocol/transport primitives with zero knowledge
   of "futures" vs "shares". Section packages (`forts/`, future `shares/`)
   own their own domain types, wire mapping, and business logic. A future
   `shares/` package must NOT import or call into `forts/*` — if two
   sections need the same behavior, the shared code belongs in an
   `internal/*` package, not in one section reused by the other.
2. **Naming mirrors the exchange, not a generic template.** ISS calls the
   derivatives section "FORTS" — so does this SDK (`forts/`), not
   `futures/` or `swap/`. The planned equities package will be `shares/`
   (ISS: `engine=stock`), not `spot/`.
3. **No import cycle between root and sections.** `moex.Client` cannot
   import `forts` (would need Config/Client types from root first). Every
   section registers a lazy factory via `moex.RegisterFortsFactory` in its
   own `init()` — identical to `okx.RegisterSwapFactory` in go-okx. Caller
   idiom: `client.Forts().(*forts.Client)`.
3. **Allocation-conscious on hot paths.** No generic/reflection-based
   codecs on the SIMBA/FIX read paths (reflection is used exactly once,
   in `internal/iss/table.go`, where ISS is inherently low-frequency).
   Hand-written FIX tag=value codec and SBE decoders instead of
   `quickfixgo` — see `internal/fix/message.go` and
   `internal/simba/packet.go` doc comments for the rationale. The SIMBA
   hot-path API is `simba.ParsePacket` + `Packet.Next` + value-type
   accessors (`Message.OrderUpdate()`, `Snapshot().Entry(i)`, ...):
   zero heap allocations per packet, enforced by
   `TestParsePacketZeroAlloc`. `DecodePacket`/`Decoded` (pointer fields,
   first message only) are compatibility wrappers — do not use them in
   new code.
4. **Wire truth comes from pcap, not from the spec.** Every change to
   `internal/simba`, `orderbook` or `forts/stream.go` must keep
   `go test ./cmd/simba-replay/` green (reference model vs. production
   fixtures) and should be re-run with `simba-replay verify` on the full
   MOEX captures. See `docs/rework-plan-2026-09-02.md` (plan and defect
   list) and `docs/pcap-findings-2026-09-02.md` (what the wire actually
   does: multi-message packets, RptSeq semantics, snapshot cycles,
   schema version 8 in production).

## 3. MOEX connectivity model (why there's no single REST+WS client)

Unlike Binance/OKX/Bybit, MOEX has no unified "API key + REST/WS" surface.
Three independent transports, each solving one concern:

| Concern | Transport | Package | Notes |
|---|---|---|---|
| Reference data, candles, history | **ISS** (HTTP/REST, JSON) | `internal/iss` | Public. Free tier: ~15 min delayed, ~10 req/s. Paid tier: passport-cookie auth (`internal/iss/auth.go`), higher limits — exact ceiling unconfirmed, see §5. |
| Order entry (New/Cancel/Replace/MassCancel) | **FIX Gate** (FIX 4.4, TCP) | `internal/fix` | Requires SenderCompID issued by MOEX/broker (see §5). No Market OrdType — see `forts/types/enums.go`. |
| Market data / order book (FORTS) | **SIMBA SPECTRA** (SBE over UDP multicast) | `internal/simba` | **Requires exchange colocation** or a broker low-latency circuit. Outside that environment, `simba.Listen` joins the socket successfully but receives nothing — this is expected, not a bug. |

Not implemented (out of v1.0 scope, listed for completeness):
**TWIME** (binary FORTS order entry, faster than FIX Gate — a v1.x upgrade
path once FIX-based trading is proven out), **Plaza II** (legacy universal
FORTS gateway), **ASTS Bridge** / equities FIX Gate (v2.0, Shares).

## 4. What v1.0 actually implements (status: code-complete, unit-tested)

- `internal/moexlog`, `internal/moexmet`, `internal/moexerr` — shared
  logging/metrics/error plumbing (mirrors go-okx's `okxlog`/`okxmet`/`okxerr`).
- Root `moex` package — `Config`/`DefaultConfig`/`Client`/`Logger`/`Error`
  public surface.
- `internal/iss` — generic columnar-JSON table parser + REST client,
  token-bucket limiter, passport cookie auth.
- `internal/fix` — FIX 4.4 tag=value codec, TCP framing, session layer
  (Logon/Heartbeat/MsgSeqNum/ResendRequest, jittered reconnect backoff).
- `internal/simba` — allocation-free packet iterator (`ParsePacket`,
  `Packet.Next`, `Message.*` accessors) over every SBE message of a
  datagram, schema-version-aware template mapping (`Kind`; versions 8 and
  9), strict header guards (MsgSize, SchemaID, Version); a **partial**
  SecurityDefinition decode (Symbol/SecurityID prefix only — see §6), UDP
  multicast listener. `Walk`/`DecodePacket` are tolerant wrappers for the
  harness and v1.0 callers.
- `internal/pcap` + `cmd/simba-replay` — offline harness over MOEX's
  public production captures (flows/stats/verify/extract) and golden
  fixtures in `internal/simba/testdata/`.
- `orderbook` — protocol-agnostic L3 engine (per-order state + L2
  aggregation, sequence-gap detection), reusable by `shares/` in v2.0.
- `forts/` — `Client` (lifecycle/Connect/Close), `TradingClient` (order
  entry via FIX, ClOrdID correlation, open-order cache, WatchOpenOrders),
  `AccountClient` (fill-derived position tracking + SeedPosition),
  `MarketDataClient` (ISS reference data/candles, SIMBA order book via
  ResolveSecurityID/WatchOrderBook).
- `examples/market-data`, `examples/trading`, `examples/orderbook` —
  runnable demonstrations of each transport tier (market-data needs no
  credentials at all; trading needs FIX Gate credentials; orderbook needs
  colocation).
- Every internal package plus `forts/` has unit tests (`go test ./...`
  passes clean under `-race`); FIX/SIMBA tests use synthetic wire bytes,
  no live connection needed.

## 5. Open questions — need input from the exchange/broker/user

These block moving from "compiles and unit-tests pass" to "trades on the
real FORTS test circuit." Tracked here so they don't get silently
forgotten between sessions.

1. **Test circuit credentials** (FIX Gate SenderCompID, host/port for the
   test environment; ISS paid-tier login). Public documentation does not
   expose test-circuit connectivity details — these must come from
   MOEX/broker directly. User: will request; agent should keep developing
   against publicly available info (live production ISS, public FIX Gate
   spec, public SIMBA schema) in the meantime — this is exactly what v1.0
   did.
2. **Colocation ETA** (required for any real SIMBA traffic). User: ETA to
   be confirmed; **decision made: SIMBA ships in v1.0 regardless** (code
   is written and unit-tested against the schema now; it simply won't
   receive UDP data until colocation/broker circuit is live — same
   "compiles, doesn't have a live socket yet" state as the FIX trading
   path before test credentials arrive).
3. **Qualified investor status.** User confirmed they are a qualified
   investor at broker BKS (БКС) and is in the process of transferring that
   status to the broker that will provide MOEX/FORTS connectivity for this
   project (status transfer between brokers is possible per user). FORTS
   crypto-index and Si futures are qualified-investor-only instruments —
   this transfer must complete before live trading, not before development.
4. **Currency hedging (SI1!/SI2!).** User flagged this specifically for
   the case where crypto-index futures (BTC/ETH/SOL/XRP/TRX) are RUB-margined
   — a USD/RUB hedge (via Si futures) would then be needed to keep USD-
   denominated crypto exposure clean of FX risk. Not yet implemented:
   this is a **trading-strategy concern for `sleipnir-trading-core`**, not
   an SDK concern — `go-moex` just needs to support trading Si alongside
   the crypto-index contracts, which it already does (no instrument-type
   restriction in `forts.TradingClient`). Revisit once margining currency
   is confirmed for the target contracts.
5. **SIMBA A/B redundancy — DONE (stage 3, 2026-09-02).** `forts.BookSession`
   joins both legs when `IncrementalGroupB`/`SnapshotGroupB` are set and
   merges them by `MsgSeqNum` in `forts/feed.go` (duplicates dropped,
   out-of-order packets held until the missing one arrives from either
   leg). Joins are source-specific (`SIMBAConfig.SourceIPA/B`, Linux
   `IP_ADD_SOURCE_MEMBERSHIP`, what MOEX's reference client does; any-source
   fallback elsewhere). Verified offline with simulated 1–5 % loss per leg on
   production captures (`simba-replay session -ab -loss ...`): zero book
   mismatches.
6. **SIMBA TCP Replay service — DONE (stage 3).** `internal/simba/replay.go`
   implements §4.2.6 (Logon/MarketDataRequest/Logout over the Snapshot packet
   framing, ≤1000 packets per request, 1 s activity timeout). The feed layer
   requests a replay when a gap stays open for `FeedConfig.GapTimeout` (20 ms)
   and falls back to the Snapshot feed if it fails or the gap is too large.
   Untested against the real endpoint (no circuit yet) — only against the
   protocol fake in `replay_test.go` and the capture-backed replayer of the
   harness.
7. **SIMBA socket buffer sizing.** `SIMBAConfig`/`ListenerConfig` expose
   `SocketReadBufferBytes` (SO_RCVBUF) but there is no MOEX-recommended
   value yet — needs to come from the colocation provisioning docs or MOEX
   support once that environment exists. Production averages 3 500
   packets/s on the Incremental feed with bursts at the open; start at
   8–16 MB.
8. **`EmptyBook` scoping — RESOLVED by captures.** `EmptyBook(4)` is a global
   "clear every book" (daily reset / clearing, spec §4.2.8); no
   per-instrument variant was observed in production traffic
   (docs/pcap-findings-2026-09-02.md). After it the Incremental feed alone
   defines the books (re-broadcast with `PossDupFlag`, `RptSeq=0`), which is
   what `BookSession` implements.
9. **`TimeInForceFOK` (tag 59 = "4").** Follows the FIX 4.4 standard
   enumeration; not explicitly confirmed against MOEX FORTS FIX Gate spec
   text at authoring time. Confirm against the test circuit once available.
10. **FIX OrdRejReason/CxlRejReason → ErrorKind mapping**
    (`internal/moexerr/errors.go`) is a best-effort mapping from the FIX
    4.4 standard enumeration; needs contract-test verification against
    real FIX Gate rejects once test-circuit access exists (crypto-index
    futures and Si specifically, since those are the target instruments).
11. **`SecurityDefinition` is only partially decoded** (Symbol + SecurityID
    prefix of the root block — see `internal/simba/types.go` doc). Margins,
    price limits, CFICode, event dates etc. are not decoded because v1.0
    sources all of that from ISS instead. Revisit if a use case needs a
    field only SIMBA carries (e.g. real-time trading-status changes
    intraday, which ISS reference data does not push).
12. **ISS candles pagination.** `GetHistoricalCandles` issues exactly one
    ISS request per call. Empirically (see §7), ISS silently ignores a
    `limit` parameter larger than its internal page size and returns
    however much history it has buffered for that interval rather than
    the exact requested count — proper pagination (loop on the `history.cursor`
    equivalent / iss.meta paging block) is a v1.x follow-up, not done here.

## 6. Known limitations (intentional trade-offs, not bugs)

- **Position tracking accuracy** (`forts/types/position-info.go`,
  `forts/account.go`): FORTS FIX Gate has no "Position Report" message.
  Positions are derived purely from Execution Reports (`ExecType=Trade`)
  observed during the current process's lifetime. Accurate for fills
  since the SDK started tracking; **wrong (starts at zero) for pre-existing
  positions** until reconciled via `AccountClient.SeedPosition` at
  startup. This is a hard external-data-source limitation, not something
  more code can fix without a Plaza II clearing feed or broker EOD report
  integration (out of scope for v1.0).
- **`orderbook.Engine` keeps price levels in two sorted slices** (stage 4):
  top of book is O(1), `TopN(side, n, dst)` copies into the caller's buffer
  without allocating, a new/emptied level costs a memmove of the side's
  levels (hundreds of entries on FORTS). `Levels(n)` allocates a fresh
  slice — convenience for non-hot callers.
- **`Engine` is not internally synchronized** — callers must serialize
  `ApplyDelta`/`LoadSnapshot`/`Clear` calls per instrument.
  `forts.BookSession` holds one mutex over all its books and invokes
  `OnBook` under it; read engines only from that callback (or under your
  own synchronisation with it).
- **One SIMBA multicast interface (`NetworkInterface`) shared by every
  `Listen` call** — fine for a single-NIC colocation host; a multi-circuit
  redundant setup would need per-group interface selection, not modeled
  in v1.0's `SIMBAConfig`.

## 7. Verified against live ISS (production, no credentials)

Confirms the SDK's column mapping and the SECID/SHORTNAME naming
inversion documented in `forts/market.go` against real data (checked
2026-07-28):

```bash
curl 'https://iss.moex.com/iss/engines/futures/markets/forts/securities.json?iss.meta=off&iss.only=securities&securities.columns=SECID,SHORTNAME,DECIMALS,MINSTEP,LOTVOLUME,STEPPRICE,LASTTRADEDATE,ASSETCODE&limit=5'
# columns: ["SECID","SHORTNAME","DECIMALS","MINSTEP","LOTVOLUME","STEPPRICE","LASTTRADEDATE","ASSETCODE"]
# row: ["SiZ6","Si-12.26",0,1.00000,1000,1.00000,"2026-12-17","Si"]
#        ^short (wire "Symbol")  ^^^^^^^^ long (used as FIX Symbol/tag 55 per spec §3.2.5 note 5)
```

```bash
curl 'https://iss.moex.com/iss/engines/futures/markets/forts/securities/SiZ6/candles.json?iss.meta=off&interval=60&limit=3'
# columns: ["open","close","high","low","value","volume","begin","end"]
```

Both confirm `forts/market.go`'s `issSecurityRow`/`issCandleRow` struct
tags and the SECID (short)/SHORTNAME (long) mapping are correct against
the live exchange, not just against the spec prose. `Si-12.26` is the
example symbol used throughout `examples/` for this reason — it is a
real, currently-listed instrument.

## 8. Next steps

1. `sleipnir-trading-core`: create branch `moex-connector` from `qa`
   (localize all MOEX-related changes there per project convention).
2. Add `internal/connectors/moex/{common,forts}` + `Exchange` enum entries
   (`moex_forts`, naming decided per §2 principle 2) + a
   `factory.ProduceExchange` case wiring `forts.Client` into the existing
   `ExchangeConnector` composite interface (`TradingConnector` +
   `MarketDataConnector` + `FuturesAccountConnector`).
3. Once test-circuit credentials arrive (§5.1): smoke-test `Connect` +
   `CreateOrder`/`CancelOrder` against the FORTS test environment, then
   revisit items in §5 that are marked "confirm against test circuit."
4. Once colocation/broker circuit is live (§5.2): validate `WatchOrderBook`
   against real SIMBA traffic, then prioritize §5 items 5/6/8 (A/B
   redundancy, TCP Replay, EmptyBook scoping) based on observed behavior
   rather than speculative correctness.
