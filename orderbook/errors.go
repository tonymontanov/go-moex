/*
FILE: orderbook/errors.go

DESCRIPTION:
Sentinel errors for gap/resync signaling. Kept as a plain exported var
(errors.Is-compatible) rather than a full moexerr.Error — this package has
no transport of its own to attribute the error to, and gap handling is a
control-flow decision the caller makes synchronously (see
ErrSequenceGap doc), not something that needs Kind/Transport metadata.
*/
package orderbook

import "errors"

// ErrSequenceGap — seq is ahead of lastSeq+1: updates were lost (spec
// §1.4.6/§4.2.5), the book is left untouched and the caller must resync
// (LoadSnapshot) before applying further deltas.
var ErrSequenceGap = errors.New("orderbook: sequence gap detected, resync required")

// ErrDuplicate — seq <= lastSeq: the update was already applied (A/B feed
// redundancy, TCP Replay overlap). The book is left untouched; callers
// count and ignore.
var ErrDuplicate = errors.New("orderbook: duplicate sequence number, already applied")

// ErrUnknownOrder — a Delete/Change for an order the book does not hold.
// The sequence number IS consumed (the update was valid on the feed). On a
// correctly synchronised book this never happens on production feeds
// (docs/pcap-findings-2026-09-02.md: 0 in 2.1M updates), so it signals
// desync — count it and consider a resync; never clear the book on it.
var ErrUnknownOrder = errors.New("orderbook: unknown order id")
