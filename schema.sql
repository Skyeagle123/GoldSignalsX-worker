CREATE TABLE IF NOT EXISTS bars (
  tf INTEGER NOT NULL DEFAULT 1,
  t INTEGER PRIMARY KEY,
  o REAL NOT NULL,
  h REAL NOT NULL,
  l REAL NOT NULL,
  c REAL NOT NULL,
  v REAL NOT NULL DEFAULT 0,
  provider TEXT NOT NULL DEFAULT 'legacy'
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_bars_t ON bars(t);
CREATE INDEX IF NOT EXISTS idx_bars_tf_t ON bars(tf, t);

CREATE TABLE IF NOT EXISTS bars_v2 (
  tf INTEGER NOT NULL,
  t INTEGER NOT NULL,
  o REAL NOT NULL,
  h REAL NOT NULL,
  l REAL NOT NULL,
  c REAL NOT NULL,
  v REAL NOT NULL DEFAULT 0,
  provider TEXT NOT NULL DEFAULT 'legacy',
  PRIMARY KEY (tf, t)
);

CREATE INDEX IF NOT EXISTS idx_bars_v2_tf_t ON bars_v2(tf, t);

-- Production-only performance ledger. Backtest output is never written here.
CREATE TABLE IF NOT EXISTS production_signals (
  signal_id TEXT PRIMARY KEY,
  source TEXT NOT NULL DEFAULT 'production' CHECK (source = 'production'),
  symbol TEXT NOT NULL DEFAULT 'XAUUSD',
  timeframe TEXT NOT NULL,
  direction TEXT NOT NULL CHECK (direction IN ('buy', 'sell')),
  created_at INTEGER NOT NULL,
  signal_bar_ts INTEGER NOT NULL DEFAULT 0,
  entry REAL NOT NULL,
  tp1 REAL NOT NULL,
  tp2 REAL NOT NULL,
  sl REAL NOT NULL,
  confidence REAL NOT NULL DEFAULT 0,
  score REAL,
  reasons_json TEXT NOT NULL DEFAULT '[]',
  status TEXT NOT NULL DEFAULT 'active',
  final_status TEXT,
  tp1_at INTEGER,
  tp1_price REAL,
  tp2_at INTEGER,
  tp2_price REAL,
  sl_at INTEGER,
  sl_price REAL,
  expired_at INTEGER,
  expired_price REAL,
  closed_at INTEGER,
  result_r REAL,
  recorded_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_production_signals_created
  ON production_signals(created_at DESC, signal_id DESC);
CREATE INDEX IF NOT EXISTS idx_production_signals_tf_created
  ON production_signals(timeframe, created_at DESC);

CREATE TABLE IF NOT EXISTS production_signal_events (
  event_id TEXT PRIMARY KEY,
  signal_id TEXT NOT NULL,
  event_type TEXT NOT NULL CHECK (event_type IN ('created', 'tp1', 'tp2', 'sl', 'expired')),
  event_at INTEGER NOT NULL,
  observed_price REAL NOT NULL,
  level REAL NOT NULL,
  source TEXT NOT NULL DEFAULT 'production_lifecycle' CHECK (source = 'production_lifecycle'),
  recorded_at INTEGER NOT NULL,
  UNIQUE(signal_id, event_type),
  FOREIGN KEY(signal_id) REFERENCES production_signals(signal_id)
);

CREATE INDEX IF NOT EXISTS idx_production_events_signal_time
  ON production_signal_events(signal_id, event_at, event_type);
