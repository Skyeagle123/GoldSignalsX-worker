-- M2 recovery only. Immutable receipt timing is processing start, not availability.
ALTER TABLE measurement_ingress_receipts RENAME COLUMN ingested_at TO processing_started_at;
CREATE TABLE measurement_ingress_recovery (
 event_id TEXT PRIMARY KEY REFERENCES measurement_ingress_receipts(event_id),
 ingested_at INTEGER CHECK(ingested_at IS NULL OR ingested_at>=0),
 processing_status TEXT NOT NULL CHECK(processing_status IN ('PENDING','CURRENT')),
 processing_gaps_json TEXT NOT NULL
);
CREATE TRIGGER measurement_availability_immutable BEFORE UPDATE OF ingested_at ON measurement_ingress_recovery
 WHEN OLD.ingested_at IS NOT NULL AND NEW.ingested_at IS NOT OLD.ingested_at
 BEGIN SELECT RAISE(ABORT,'measurement_availability_immutable'); END;
CREATE TRIGGER measurement_availability_no_delete BEFORE DELETE ON measurement_ingress_recovery
 BEGIN SELECT RAISE(ABORT,'measurement_availability_immutable'); END;
CREATE TABLE measurement_decision_bindings (
 evaluation_id TEXT PRIMARY KEY,
 payload_digest TEXT NOT NULL CHECK(length(payload_digest)=64),
 official_signal_id TEXT,
 first_event_id TEXT NOT NULL REFERENCES measurement_ingress_receipts(event_id)
);
CREATE INDEX measurement_decision_signal ON measurement_decision_bindings(official_signal_id);
CREATE TRIGGER measurement_decision_binding_conflict BEFORE INSERT ON measurement_decision_bindings
 WHEN EXISTS(SELECT 1 FROM measurement_decision_bindings b WHERE b.evaluation_id=NEW.evaluation_id AND b.payload_digest!=NEW.payload_digest)
 BEGIN SELECT RAISE(ABORT,'measurement_decision_integrity_conflict'); END;
CREATE TRIGGER measurement_decision_binding_immutable BEFORE UPDATE ON measurement_decision_bindings
 BEGIN SELECT RAISE(ABORT,'measurement_decision_binding_immutable'); END;
CREATE TRIGGER measurement_decision_binding_no_delete BEFORE DELETE ON measurement_decision_bindings
 BEGIN SELECT RAISE(ABORT,'measurement_decision_binding_immutable'); END;
CREATE TABLE measurement_decision_conflicts (
 evaluation_id TEXT NOT NULL REFERENCES measurement_decision_bindings(evaluation_id),
 rejected_digest TEXT NOT NULL CHECK(length(rejected_digest)=64),
 detected_at INTEGER NOT NULL,
 PRIMARY KEY(evaluation_id,rejected_digest)
);
CREATE TRIGGER measurement_decision_conflict_immutable BEFORE UPDATE ON measurement_decision_conflicts
 BEGIN SELECT RAISE(ABORT,'measurement_decision_conflict_immutable'); END;
CREATE TRIGGER measurement_decision_conflict_no_delete BEFORE DELETE ON measurement_decision_conflicts
 BEGIN SELECT RAISE(ABORT,'measurement_decision_conflict_immutable'); END;
