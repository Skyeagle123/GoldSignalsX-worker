-- Explicit, additive D1 migration. Not a Durable Object migration.
-- Apply only after Owner authorization; never invoke from Worker GET.
CREATE TABLE measurement_cohorts (
 cohort_id TEXT PRIMARY KEY, schema_version INTEGER NOT NULL CHECK(schema_version=1),
 effective_at INTEGER NOT NULL, payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
 payload_digest TEXT NOT NULL, recorded_at INTEGER NOT NULL, persisted_at INTEGER NOT NULL DEFAULT(CAST((julianday('now')-2440587.5)*86400000 AS INTEGER))
);
CREATE TABLE market_evidence_blocks (
 block_id TEXT PRIMARY KEY, timeframe TEXT NOT NULL, from_at INTEGER, to_at INTEGER,
 payload_json TEXT NOT NULL CHECK(json_valid(payload_json)), payload_digest TEXT NOT NULL,
 recorded_at INTEGER NOT NULL, persisted_at INTEGER NOT NULL DEFAULT(CAST((julianday('now')-2440587.5)*86400000 AS INTEGER))
);
CREATE TABLE decision_cycle_evidence (
 cycle_id TEXT PRIMARY KEY, cohort_id TEXT NOT NULL REFERENCES measurement_cohorts(cohort_id),
 evaluated_at INTEGER NOT NULL, payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
 block_ids_json TEXT NOT NULL CHECK(json_valid(block_ids_json)), payload_digest TEXT NOT NULL, recorded_at INTEGER NOT NULL, persisted_at INTEGER NOT NULL DEFAULT(CAST((julianday('now')-2440587.5)*86400000 AS INTEGER))
);
CREATE TABLE signal_decision_evidence (
 evaluation_id TEXT PRIMARY KEY, candidate_key TEXT, official_signal_id TEXT,
 kind TEXT NOT NULL CHECK(kind IN ('OFFICIAL','CANDIDATE')), cycle_id TEXT NOT NULL REFERENCES decision_cycle_evidence(cycle_id),
 cohort_id TEXT NOT NULL REFERENCES measurement_cohorts(cohort_id), timeframe TEXT NOT NULL,
 evaluated_at INTEGER NOT NULL, measurement_only INTEGER NOT NULL CHECK(measurement_only=1),
 decision_use INTEGER NOT NULL CHECK(decision_use=0), payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
 payload_digest TEXT NOT NULL, recorded_at INTEGER NOT NULL, persisted_at INTEGER NOT NULL DEFAULT(CAST((julianday('now')-2440587.5)*86400000 AS INTEGER)),
 CHECK((kind='OFFICIAL' AND official_signal_id IS NOT NULL) OR (kind='CANDIDATE' AND official_signal_id IS NULL))
);
CREATE UNIQUE INDEX measurement_official_identity ON signal_decision_evidence(official_signal_id) WHERE kind='OFFICIAL';
CREATE INDEX measurement_decisions_cohort_time ON signal_decision_evidence(cohort_id,kind,evaluated_at,evaluation_id);
CREATE INDEX measurement_candidate_attempts ON signal_decision_evidence(candidate_key,evaluated_at);
CREATE TABLE signal_outcome_evidence (
 event_id TEXT PRIMARY KEY, subject_id TEXT NOT NULL, event_type TEXT NOT NULL,
 occurred_at INTEGER, available_at INTEGER NOT NULL, payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
 payload_digest TEXT NOT NULL, block_ids_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(block_ids_json)), recorded_at INTEGER NOT NULL, persisted_at INTEGER NOT NULL DEFAULT(CAST((julianday('now')-2440587.5)*86400000 AS INTEGER))
);
CREATE INDEX measurement_outcomes_subject_time ON signal_outcome_evidence(subject_id,available_at,event_id);
CREATE TABLE signal_measurement_state (
 subject_id TEXT PRIMARY KEY, cohort_id TEXT, state_version INTEGER NOT NULL CHECK(state_version=1),
 payload_json TEXT NOT NULL CHECK(json_valid(payload_json)), updated_at INTEGER NOT NULL,
 evaluation_id TEXT REFERENCES signal_decision_evidence(evaluation_id),
 kind TEXT CHECK(kind IN ('OFFICIAL','CANDIDATE')), next_observe_at INTEGER, observation_end_at INTEGER
);
CREATE INDEX measurement_state_cursor ON signal_measurement_state(updated_at,subject_id);
CREATE INDEX measurement_collection_due ON signal_measurement_state(kind,next_observe_at,subject_id);
CREATE TRIGGER measurement_cohort_immutable BEFORE UPDATE ON measurement_cohorts BEGIN SELECT RAISE(ABORT,'immutable_measurement_evidence'); END;
CREATE TRIGGER measurement_market_immutable BEFORE UPDATE ON market_evidence_blocks BEGIN SELECT RAISE(ABORT,'immutable_measurement_evidence'); END;
CREATE TRIGGER measurement_cycle_immutable BEFORE UPDATE ON decision_cycle_evidence BEGIN SELECT RAISE(ABORT,'immutable_measurement_evidence'); END;
CREATE TRIGGER measurement_decision_immutable BEFORE UPDATE ON signal_decision_evidence BEGIN SELECT RAISE(ABORT,'immutable_measurement_evidence'); END;
CREATE TRIGGER measurement_outcome_immutable BEFORE UPDATE ON signal_outcome_evidence BEGIN SELECT RAISE(ABORT,'immutable_measurement_evidence'); END;
-- Normalized reachability for bounded, indexed garbage collection. Payload arrays
-- remain authoritative manifests; this index does not affect trading decisions.
CREATE TABLE market_evidence_references (
 owner_type TEXT NOT NULL CHECK(owner_type IN ('CYCLE','OUTCOME')),
 owner_id TEXT NOT NULL, block_id TEXT NOT NULL REFERENCES market_evidence_blocks(block_id),
 PRIMARY KEY(owner_type,owner_id,block_id)
);
CREATE INDEX measurement_market_reference_block ON market_evidence_references(block_id);
CREATE INDEX measurement_decision_cycle ON signal_decision_evidence(cycle_id);
CREATE TRIGGER measurement_cycle_reference_cleanup AFTER DELETE ON decision_cycle_evidence BEGIN
 DELETE FROM market_evidence_references WHERE owner_type='CYCLE' AND owner_id=OLD.cycle_id;
END;
CREATE TRIGGER measurement_outcome_reference_cleanup AFTER DELETE ON signal_outcome_evidence BEGIN
 DELETE FROM market_evidence_references WHERE owner_type='OUTCOME' AND owner_id=OLD.event_id;
END;

CREATE INDEX measurement_candidate_retention ON signal_decision_evidence(kind,evaluated_at,evaluation_id);
CREATE INDEX measurement_cycle_retention ON decision_cycle_evidence(evaluated_at,cycle_id);
CREATE INDEX measurement_market_retention ON market_evidence_blocks(recorded_at,block_id);

CREATE TABLE measurement_schema_meta (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL,
 capture_contract TEXT NOT NULL, outcome_reducer TEXT NOT NULL
);
INSERT INTO measurement_schema_meta(singleton,version,capture_contract,outcome_reducer)
 VALUES(1,1,'b1-capture-v1','tp1-first-v1');
