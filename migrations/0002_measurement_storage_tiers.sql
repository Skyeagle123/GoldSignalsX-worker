-- Additive storage remediation; explicit migration only, never request-time DDL.
CREATE TABLE measurement_definitions (
 definition_id TEXT PRIMARY KEY, payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
 payload_digest TEXT NOT NULL, recorded_at INTEGER NOT NULL
);
CREATE TRIGGER measurement_definition_immutable BEFORE UPDATE ON measurement_definitions BEGIN SELECT RAISE(ABORT,'immutable_measurement_evidence'); END;
CREATE TABLE measurement_rich_evidence (
 evidence_id TEXT PRIMARY KEY, owner_type TEXT NOT NULL, owner_id TEXT NOT NULL,
 payload_json TEXT NOT NULL CHECK(json_valid(payload_json)), payload_digest TEXT NOT NULL,
 recorded_at INTEGER NOT NULL, retain_until INTEGER
);
CREATE INDEX measurement_rich_retention ON measurement_rich_evidence(retain_until,evidence_id) WHERE retain_until IS NOT NULL;
CREATE TRIGGER measurement_rich_immutable BEFORE UPDATE ON measurement_rich_evidence BEGIN SELECT RAISE(ABORT,'immutable_measurement_evidence'); END;
CREATE TABLE measurement_final_results (
 subject_id TEXT PRIMARY KEY, payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
 payload_digest TEXT NOT NULL, recorded_at INTEGER NOT NULL, retain_until INTEGER
);
CREATE INDEX measurement_final_retention ON measurement_final_results(retain_until,subject_id) WHERE retain_until IS NOT NULL;
CREATE TRIGGER measurement_final_immutable BEFORE UPDATE ON measurement_final_results BEGIN SELECT RAISE(ABORT,'immutable_measurement_evidence'); END;
CREATE TABLE measurement_official_pins (
 official_signal_id TEXT NOT NULL, block_id TEXT NOT NULL REFERENCES market_evidence_blocks(block_id),
 PRIMARY KEY(official_signal_id,block_id)
);
CREATE INDEX measurement_pin_block ON measurement_official_pins(block_id);
UPDATE measurement_schema_meta SET version=2,capture_contract='b1-capture-v2';
