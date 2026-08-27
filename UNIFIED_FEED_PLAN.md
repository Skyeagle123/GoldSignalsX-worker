# GoldSignalsX Ultimate — Unified Twelve Data Feed

Working branch: `fix/twelve-data-unified-market-feed`

## Fixed architecture

1. Twelve Data is the primary live market-data provider for XAU/USD.
2. Live price and analytical candles must come from the same provider lineage.
3. D1 is storage only; it must not be presented as a market-data provider.
4. `gold-api` may be used only as a display-only emergency fallback and must never be mixed with Twelve Data candles to issue a trading signal.
5. Build/store canonical 1-minute candles, then resample them for 5m, 15m, 30m, 60m, 240m and 1d.
6. REST `time_series` is for startup/recovery/backfill, not continuous polling.
7. After a provider disconnect/recovery, suppress new signals until data continuity is validated and at least two complete candles have been observed.

## Data-safety checks before signals

- Last candle must be complete, not currently forming.
- Candle timestamps must be strictly increasing.
- Reject duplicate or missing minute buckets for the signal lookback window.
- Live price and candles must have the same provider lineage.
- Live price must be reasonably close to the latest candle close.
- Do not resume signals immediately after a feed interruption.

## Follow-up phases

- Move signal monitoring/calculation from the browser into the Worker.
- Persist every emitted signal and its eventual outcome.
- Replace confidence labels with metrics backed by recorded/backtested results.
- Expand backtest history and automatic regression tests.
- Improve gold-news sourcing, economic-calendar awareness, and impact scoring.
