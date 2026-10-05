-- M2 measurement-plane foundation only. Apply to the separate measurement DB.
-- Historical B1 schema/codec version 4 remains unchanged.
CREATE TABLE measurement_ingress_receipts (
 event_id TEXT PRIMARY KEY,
 semantic_key TEXT NOT NULL UNIQUE,
 semantic_id TEXT NOT NULL,
 event_kind TEXT NOT NULL,
 producer_namespace TEXT NOT NULL CHECK(producer_namespace='goldsignalsx:b1'),
 envelope_version INTEGER NOT NULL CHECK(envelope_version=1),
 payload_digest TEXT NOT NULL CHECK(length(payload_digest)=64),
 occurred_at INTEGER,
 observed_at INTEGER,
 prepared_at INTEGER,
 clock_unavailable INTEGER NOT NULL,
 received_at INTEGER NOT NULL,
 ingested_at INTEGER NOT NULL CHECK(ingested_at>=received_at),
 ingestion_status TEXT NOT NULL CHECK(ingestion_status='ACCEPTED')
);
CREATE INDEX measurement_ingress_available ON measurement_ingress_receipts(event_kind,semantic_id,ingested_at);
CREATE TRIGGER measurement_ingress_conflict BEFORE INSERT ON measurement_ingress_receipts
 WHEN EXISTS(SELECT 1 FROM measurement_ingress_receipts r WHERE (r.event_id=NEW.event_id OR r.semantic_key=NEW.semantic_key)
 AND (r.event_id!=NEW.event_id OR r.payload_digest!=NEW.payload_digest OR r.semantic_key!=NEW.semantic_key))
 BEGIN SELECT RAISE(ABORT,'measurement_ingress_integrity_conflict'); END;
CREATE TRIGGER measurement_ingress_immutable BEFORE UPDATE ON measurement_ingress_receipts
 BEGIN SELECT RAISE(ABORT,'measurement_ingress_immutable'); END;
CREATE TRIGGER measurement_ingress_no_delete BEFORE DELETE ON measurement_ingress_receipts
 BEGIN SELECT RAISE(ABORT,'measurement_ingress_immutable'); END;

CREATE TABLE measurement_lifecycle_facts (
 event_id TEXT PRIMARY KEY REFERENCES measurement_ingress_receipts(event_id),
 signal_id TEXT,
 fact_kind TEXT NOT NULL CHECK(fact_kind IN ('OFFICIAL_CREATION','LIFECYCLE_FACT','CONFIRMATION_LINK','CAPTURE_GAP')),
 payload_blob BLOB NOT NULL,
 codec TEXT NOT NULL,
 uncompressed_length INTEGER NOT NULL,
 payload_digest BLOB NOT NULL
);
CREATE INDEX measurement_lifecycle_subject ON measurement_lifecycle_facts(signal_id,event_id);
CREATE TRIGGER measurement_lifecycle_immutable BEFORE UPDATE ON measurement_lifecycle_facts
 BEGIN SELECT RAISE(ABORT,'measurement_lifecycle_immutable'); END;
CREATE TRIGGER measurement_lifecycle_no_delete BEFORE DELETE ON measurement_lifecycle_facts
 BEGIN SELECT RAISE(ABORT,'measurement_lifecycle_immutable'); END;

-- Cache only: the facts and their first-ingestion clocks are authoritative.
CREATE TABLE measurement_lifecycle_projection (
 signal_id TEXT PRIMARY KEY,
 status TEXT,
 closed_at INTEGER,
 created_at INTEGER,
 timeframe TEXT,
 direction TEXT,
 entry REAL,
 tp1 REAL,
 tp2 REAL,
 sl REAL,
 available_at INTEGER NOT NULL,
 fact_count INTEGER NOT NULL,
 integrity_status TEXT NOT NULL CHECK(integrity_status IN ('COMPLETE','PARTIAL','CONFLICT')),
 payload_blob BLOB NOT NULL,
 codec TEXT NOT NULL,
 uncompressed_length INTEGER NOT NULL,
 payload_digest BLOB NOT NULL,
 rebuilt_at INTEGER NOT NULL
);
