# go-moex

A Go SDK for trading and market data on the Moscow Exchange (MOEX). Built
for high-frequency, low-latency trading infrastructure, in the same style
as [go-okx](https://github.com/tonymontanov/go-okx) and
[go-bybit](https://github.com/tonymontanov/go-bybit): a shared,
allocation-conscious transport layer underneath section-specific clients
that mirror the exchange's own naming and semantics.

Unlike Binance/OKX/Bybit, MOEX has no single "API key + REST/WS" surface.
`go-moex` talks to three independent MOEX transports directly:

| Transport | Purpose | Access model |
|---|---|---|
| **ISS** (HTTP/REST) | Reference data, historical candles | Public, no credentials (free tier: ~15 min delayed) |
| **TWIME SPECTRA** (SBE / TCP) | Order entry (New/Cancel/Replace/MassCancel), low-latency gateway | Requires an exchange-issued TWIME login (IP-authenticated); selected by `Config.TWIME.Addr` |
| **FIX Gate** (FIX 4.4 / TCP) | Order entry, fallback transport | Requires a broker/exchange-issued FIX session |
| **SIMBA SPECTRA** (SBE / UDP multicast) | Live order book | Requires exchange colocation or a broker low-latency circuit |

See [`docs/handoff.md`](docs/handoff.md) for the full connectivity model,
architecture rationale, current limitations, and open questions.

## Status

**v1.0 — FORTS (Derivatives market: futures + options) MVP.** Code-complete
and unit-tested (`go test ./...`, clean under `-race`); order entry and
live market data require exchange/broker-issued credentials and
colocation respectively to actually exchange bytes with MOEX — see
`docs/handoff.md` §5 "Open questions."

Planned: v2.0 (Shares/equities), v2.5 (full exchange coverage).

## Install

```bash
go get github.com/tonymontanov/go-moex
```

## Quick start — market data (no credentials required)

```go
package main

import (
	"context"
	"fmt"
	"log"

	moex "github.com/tonymontanov/go-moex"
	"github.com/tonymontanov/go-moex/forts"
)

func main() {
	client, err := moex.NewClient(moex.DefaultConfig())
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	fortsClient := client.Forts().(*forts.Client)

	symbols, err := fortsClient.MarketData().GetSymbols(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("FORTS lists %d instruments\n", len(symbols))
}
```

Runnable examples covering all three transports live in
[`examples/`](examples/):

- [`examples/market-data`](examples/market-data) — reference data and
  candles via ISS. Works with zero setup.
- [`examples/trading`](examples/trading) — order entry over TWIME
  (`MOEX_TWIME_ADDR` / `MOEX_TWIME_LOGIN` / `MOEX_TWIME_ACCOUNT` /
  `MOEX_TWIME_SECURITY_ID`) or FIX Gate (`MOEX_FIX_SENDER_COMP_ID` /
  `MOEX_FIX_ACCOUNT`); same code path either way.
- [`examples/orderbook`](examples/orderbook) — live order book via SIMBA
  SPECTRA. Requires exchange colocation and
  `MOEX_SIMBA_INSTRUMENTS_GROUP_A` / `MOEX_SIMBA_INCREMENTAL_GROUP_A`.

## Architecture

```
moex.Client (root)
  └── Forts() → forts.Client            (lazy, registered via init())
        ├── MarketDataClient   — ISS reference data/candles, SIMBA order book
        ├── TradingClient      — order entry over TWIME (Config.TWIME.Addr set) or FIX Gate
        └── AccountClient      — fill-derived position tracking

internal/iss     — generic ISS REST client (columnar JSON table parser, rate limiting, auth)
internal/fix     — FIX 4.4 tag=value codec + session layer (Logon/Heartbeat/seq/reconnect)
internal/simba   — SBE binary decoders + UDP multicast listener for SIMBA SPECTRA
internal/sbe     — SBE primitives shared by SIMBA and TWIME (messageHeader, Decimal5, LE accessors)
internal/twime   — TWIME SPECTRA codec (schema 7.7) + TCP session layer + per-second pacer; forts/twime.go is the order-entry leg on top of it (selected by Config.TWIME.Addr)
orderbook        — protocol-agnostic L3 order book engine (shared by every section)
```

`internal/*` and `orderbook` know nothing about "FORTS" vs. a future
"Shares" section — they are pure transport/protocol primitives. Each
market section (`forts/`, and eventually `shares/`) builds its own domain
types and business logic on top, without reusing another section's code.
See `docs/handoff.md` §2 for the full rationale.

## Package layout

```
go-moex/
├── client.go, config.go, errors.go, logger.go, metrics.go, doc.go   # root SDK surface
├── types/                 # shared types (Timeframe, Candle, OrderBookLevel)
├── forts/                 # FORTS (futures+options) section client
│   └── types/              # FORTS-specific domain types (Side, OrdStatus, OrderInfo, ...)
├── orderbook/              # protocol-agnostic L3 order book engine
├── internal/
│   ├── iss/                 # MOEX ISS REST client
│   ├── fix/                  # FIX 4.4 codec + session
│   ├── simba/                 # SIMBA SPECTRA SBE decoder + UDP listener
│   ├── moexlog/, moexmet/, moexerr/  # shared logging/metrics/error plumbing
├── examples/                # runnable usage examples (see above)
└── docs/
    ├── handoff.md            # architecture, status, open questions — read this first
    └── TS-SINGLE-EXCHANGE-SDK*.md  # original technical specification (EN/RU)
```

## Testing

```bash
go test ./...          # unit tests, no network/credentials required
go test -race ./...    # same, with the race detector
```

FIX and SIMBA tests exercise the codecs against synthetic wire bytes (no
live connection needed). ISS column mapping has been additionally spot-
checked against the live production endpoint — see `docs/handoff.md` §7.

## License

See [`LICENSE`](LICENSE).
