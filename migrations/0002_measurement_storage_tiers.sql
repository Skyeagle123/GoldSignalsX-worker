-- Additive storage remediation; explicit migration only, never request-time DDL.
CREATE TABLE measurement_definitions (
 definition_id TEXT PRIMARY KEY, payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
 payload_digest TEXT NOT NULL, recorded_at INTEGER NOT NULL
);
CREATE TRIGGER measurement_definition_immutable BEFORE UPDATE ON measurement_definitions BEGIN SELECT RAISE(ABORT,'immutable_measurement_evidence'); END;
CREATE TABLE measurement_rich_evidence (
 evidence_id TEXT PRIMARY KEY, owner_type TEXT NOT NULL, owner_id TEXT NOT NULL,
 payload_json TEXT NOT NULL CHECK(json_valid(payload_json)), payload_digest TEXT NOT NULL,
 payload_blob BLOB NOT NULL, codec TEXT NOT NULL, uncompressed_length INTEGER NOT NULL,
 recorded_at INTEGER NOT NULL, retain_until INTEGER
);
CREATE INDEX measurement_rich_retention ON measurement_rich_evidence(retain_until,evidence_id) WHERE retain_until IS NOT NULL;
CREATE TRIGGER measurement_rich_immutable BEFORE UPDATE ON measurement_rich_evidence BEGIN SELECT RAISE(ABORT,'immutable_measurement_evidence'); END;
CREATE TABLE measurement_final_results (
 subject_id TEXT PRIMARY KEY, directional INTEGER NOT NULL, extended INTEGER NOT NULL, terminal INTEGER NOT NULL,
 coverage INTEGER NOT NULL, ordering INTEGER NOT NULL, horizon INTEGER, occurred_from INTEGER, occurred_to INTEGER,
 mfe REAL, mae REAL, mfe_r REAL, mae_r REAL, tp1_min REAL, tp1_max REAL, sl_min REAL, sl_max REAL,
 numeric_flags INTEGER NOT NULL, evidence_as_of INTEGER, reducer_version INTEGER NOT NULL,
 payload_digest BLOB NOT NULL, recorded_at INTEGER NOT NULL, retain_until INTEGER,
 payload_json TEXT GENERATED ALWAYS AS (json_object('outcome',json_object(
 'directionalOutcome',CASE directional WHEN 0 THEN 'SUCCESS' WHEN 1 THEN 'FAILURE' WHEN 2 THEN 'NO_DECISION' WHEN 3 THEN 'INSUFFICIENT_DATA' ELSE 'UNRESOLVED' END,
 'extendedOutcome',CASE extended WHEN 0 THEN 'TP2_REACHED' WHEN 1 THEN 'NOT_REACHED' ELSE 'UNRESOLVED' END,
 'terminalLifecycleOutcome',CASE terminal WHEN 0 THEN 'TP2' WHEN 1 THEN 'SL' WHEN 2 THEN 'EXPIRED' WHEN 3 THEN 'CLOSED_OTHER' ELSE 'ACTIVE' END,
 'timeToTp1',CASE WHEN tp1_min IS NULL THEN NULL ELSE json_object('minMs',tp1_min,'maxMs',tp1_max) END,
 'timeToSl',CASE WHEN sl_min IS NULL THEN NULL ELSE json_object('minMs',sl_min,'maxMs',sl_max) END),
 'global',json_object('mfe',mfe,'mae',mae,'mfeR',mfe_r,'maeR',mae_r,'coverage',CASE coverage WHEN 0 THEN 'COMPLETE' WHEN 1 THEN 'OBSERVED_LOWER_BOUND' ELSE 'INSUFFICIENT' END),
 'availableAt',evidence_as_of)) VIRTUAL
);
CREATE INDEX measurement_final_retention ON measurement_final_results(retain_until,subject_id) WHERE retain_until IS NOT NULL;
CREATE TRIGGER measurement_final_immutable BEFORE UPDATE ON measurement_final_results BEGIN SELECT RAISE(ABORT,'immutable_measurement_evidence'); END;
CREATE TABLE measurement_official_pins (
 official_signal_id TEXT NOT NULL, block_id TEXT NOT NULL REFERENCES market_evidence_blocks(block_id),
 PRIMARY KEY(official_signal_id,block_id)
);
CREATE INDEX measurement_pin_block ON measurement_official_pins(block_id);
UPDATE measurement_schema_meta SET version=3,capture_contract='b1-capture-v3';

-- Scalar fallback retains actual Official levels even if legacy persistence failed.
CREATE TABLE measurement_official_levels (
 official_signal_id TEXT PRIMARY KEY, entry REAL, tp1 REAL, tp2 REAL, sl REAL
);
CREATE TRIGGER measurement_official_levels_immutable BEFORE UPDATE ON measurement_official_levels BEGIN SELECT RAISE(ABORT,'immutable_measurement_evidence'); END;
-- Internal row references are storage addresses, never ownership generations.
CREATE TABLE measurement_market_links (
 owner_kind INTEGER NOT NULL CHECK(owner_kind IN (1,2)), owner_ref INTEGER NOT NULL,
 block_ref INTEGER NOT NULL, PRIMARY KEY(owner_kind,owner_ref,block_ref)
) WITHOUT ROWID;
CREATE INDEX measurement_links_block ON measurement_market_links(block_ref);
CREATE INDEX measurement_outcome_retention ON signal_outcome_evidence(recorded_at,event_id);

ALTER TABLE signal_outcome_evidence ADD COLUMN payload_blob BLOB;
ALTER TABLE signal_outcome_evidence ADD COLUMN codec TEXT;
ALTER TABLE signal_outcome_evidence ADD COLUMN uncompressed_length INTEGER;

-- Preserve any pre-tier evidence; compact storage has the same logical read shape.
ALTER TABLE signal_outcome_evidence RENAME TO measurement_legacy_outcome_evidence;
CREATE TABLE measurement_subjects (
 subject_ref INTEGER PRIMARY KEY AUTOINCREMENT, subject_id TEXT NOT NULL UNIQUE,
 recorded_at INTEGER NOT NULL, persisted_at INTEGER NOT NULL DEFAULT(CAST((julianday('now')-2440587.5)*86400000 AS INTEGER))
);
CREATE TABLE measurement_outcome_records (
 record_id INTEGER PRIMARY KEY, subject_ref INTEGER NOT NULL REFERENCES measurement_subjects(subject_ref),
 event_suffix TEXT, full_event_id TEXT, event_kind INTEGER NOT NULL, event_label TEXT,
 occurred_at INTEGER, available_offset INTEGER NOT NULL, payload_blob BLOB NOT NULL,
 payload_digest BLOB NOT NULL, uncompressed_length INTEGER NOT NULL,
 recorded_offset INTEGER NOT NULL, retention_bucket INTEGER NOT NULL, persisted_offset INTEGER NOT NULL,
 CHECK((event_suffix IS NULL)!=(full_event_id IS NULL)),
 UNIQUE(subject_ref,event_suffix), UNIQUE(full_event_id)
);
CREATE TRIGGER measurement_outcome_record_immutable BEFORE UPDATE ON measurement_outcome_records BEGIN SELECT RAISE(ABORT,'immutable_measurement_evidence'); END;
CREATE INDEX measurement_outcome_record_subject_time ON measurement_outcome_records(subject_ref,available_offset,record_id);
CREATE INDEX measurement_outcome_record_retention ON measurement_outcome_records(retention_bucket,record_id);
CREATE VIEW signal_outcome_evidence AS
 SELECT r.record_id AS rowid,COALESCE(r.full_event_id,s.subject_id||':'||r.event_suffix) AS event_id,s.subject_id,
 COALESCE(r.event_label,CASE r.event_kind WHEN 0 THEN 'TP1' WHEN 1 THEN 'TP2' WHEN 2 THEN 'SL' WHEN 3 THEN 'WINDOW_FINALIZED' WHEN 4 THEN 'FINAL_MEASUREMENT' WHEN 5 THEN 'COVERAGE_CHECKPOINT' WHEN 6 THEN 'EXPIRED' WHEN 7 THEN 'TP2_REACHED' ELSE 'CLOSED_OTHER' END) AS event_type,
 r.occurred_at,s.recorded_at+r.available_offset AS available_at,'{}' AS payload_json,r.payload_digest,'[]' AS block_ids_json,s.recorded_at+r.recorded_offset AS recorded_at,s.persisted_at+r.persisted_offset AS persisted_at,r.payload_blob,'gzip-outcome-binary-v5' AS codec,r.uncompressed_length
 FROM measurement_outcome_records r JOIN measurement_subjects s ON s.subject_ref=r.subject_ref
 UNION ALL SELECT rowid,event_id,subject_id,event_type,occurred_at,available_at,payload_json,payload_digest,block_ids_json,recorded_at,persisted_at,payload_blob,codec,uncompressed_length FROM measurement_legacy_outcome_evidence;

-- Bounded mutable recovery state; analytical projections do not copy the accumulator.
ALTER TABLE signal_measurement_state ADD COLUMN payload_blob BLOB;
ALTER TABLE signal_measurement_state ADD COLUMN codec TEXT;
ALTER TABLE signal_measurement_state ADD COLUMN uncompressed_length INTEGER;
ALTER TABLE signal_measurement_state ADD COLUMN payload_digest BLOB;

-- Storage-address reachability follows physical row deletion, including retention.
CREATE TRIGGER measurement_compact_outcome_reference_cleanup AFTER DELETE ON measurement_outcome_records BEGIN
 DELETE FROM measurement_market_links WHERE owner_kind=2 AND owner_ref=OLD.record_id;
END;
CREATE TRIGGER measurement_compact_cycle_reference_cleanup AFTER DELETE ON decision_cycle_evidence BEGIN
 DELETE FROM measurement_market_links WHERE owner_kind=1 AND owner_ref=OLD.rowid;
END;
