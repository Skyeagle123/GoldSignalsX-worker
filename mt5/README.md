# GoldSignalsX MT5 read-only price bridge

`GoldSignalsXPriceBridge.mq5` reads only the selected MT5 symbol's latest
`Bid`, `Ask`, quote timestamp, and read-only symbol specification metadata. It does not import the MT5 trading library,
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

The first request in each bridge session and a periodic refresh controlled by
`InpMetadataIntervalMs` include the broker's symbol specification. Metadata
is tied to the bridge session and is advisory only. Invalid metadata never
rejects an otherwise valid price tick. The bridge sends account currency only;
it never sends account number, login, balance, equity, orders, or credentials.

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
  "source": "mt5",
  "symbolMeta": {
    "source": "mt5",
    "canonicalSymbol": "XAUUSD",
    "brokerSymbol": "XAUUSDs",
    "sessionId": "bridge-start-id",
    "observedAt": 1789328406200,
    "contractSize": 100,
    "tickSize": 0.01,
    "tickValue": 1,
    "tickValueProfit": 1,
    "tickValueLoss": 1,
    "volumeMin": 0.01,
    "volumeMax": 100,
    "volumeStep": 0.01,
    "volumeLimit": 0,
    "point": 0.01,
    "digits": 2,
    "tradeStopsLevel": 0,
    "profitCurrency": "USD",
    "accountCurrency": "USD",
    "tradeCalcMode": "SYMBOL_CALC_MODE_CFD",
    "tradeMode": 4
  }
}
```

The token is sent only in the `X-MT5-Token` request header. Responses and EA
logs never include it.


## Server-side risk sizing

`POST /risk-sizing` accepts only the displayed official `signalId`,
`accountValueUsd`, `accountBasis`, and `riskPercent`. Entry, targets, and
stop are always loaded server-side from the matching active Primary Signal and
Exposure. The response is advisory and side-effect free:
`measurementOnly=true`, `decisionUse=false`, and `executionEnabled=false`.

Position size is returned only while the current MT5 session and its symbol
metadata are healthy. Estimates exclude commission, slippage, and swap.
