# GoldSignalsX MT5 read-only price bridge

`GoldSignalsXPriceBridge.mq5` reads only the selected MT5 symbol's latest
`Bid`, `Ask`, and quote timestamp. It does not import the MT5 trading library,
inspect account credentials, place orders, modify positions, or manage trades.

## Local setup

1. Create the Cloudflare Worker secret `MT5_INGEST_TOKEN` outside Git. Never
   paste its value into this repository.
2. In MT5, add the Worker origin to **Tools → Options → Expert Advisors → Allow
   WebRequest for listed URL**.
3. Compile `GoldSignalsXPriceBridge.mq5` in MetaEditor and attach it to the
   broker's gold chart.
4. Set `InpWorkerUrl` to the deployed Worker's `/mt5/tick` endpoint and enter
   the token only in the EA's local `InpMt5Token` input.
5. If the broker uses a suffix or a different gold symbol, set `InpMt5Symbol`.
   Keep `InpCanonicalSymbol` as `XAUUSD`.

Each timer event sends a heartbeat with a monotonically increasing sequence.
An unchanged MT5 quote is treated by the Worker as a heartbeat only. The
Worker accepts a trading tick only when its quote timestamp advances and the
XAU/USD market session is open.

## Payload

```json
{
  "symbol": "XAUUSD",
  "bid": 4348.31,
  "ask": 4348.48,
  "mt5Time": 1789328406000,
  "sentAt": 1789328406200,
  "sequence": 123456,
  "sessionId": "bridge-start-id",
  "source": "mt5"
}
```

The token is sent only in the `X-MT5-Token` request header. Responses and EA
logs never include it.
