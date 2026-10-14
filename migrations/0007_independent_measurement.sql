-- Package A only. Additive to accepted 0001--0006. Never run from fetch/queue.
-- pa_* is the independent cohort: accepted native M2 tables remain untouched.
CREATE TABLE pa_meta(singleton INTEGER PRIMARY KEY CHECK(singleton=1),schema_version INTEGER NOT NULL DEFAULT 7,
 epoch TEXT NOT NULL,revision INTEGER NOT NULL DEFAULT 0,live_floor INTEGER NOT NULL DEFAULT 0,catalogue_generation INTEGER NOT NULL DEFAULT 0,
 root_key TEXT,scheduler_cursor INTEGER NOT NULL DEFAULT 0,resource_revision INTEGER NOT NULL DEFAULT 0,deletion_generation INTEGER NOT NULL DEFAULT 0,physical_observed INTEGER,physical_charged INTEGER NOT NULL DEFAULT 0 CHECK(physical_charged>=0));
INSERT INTO pa_meta(singleton,epoch) VALUES(1,lower(hex(randomblob(16))));
CREATE TABLE pa_assertions(ok INTEGER NOT NULL);
CREATE TRIGGER pa_assert BEFORE INSERT ON pa_assertions WHEN NEW.ok IS NOT 1 BEGIN SELECT RAISE(ABORT,'pa_fence_failed'); END;
CREATE TABLE pa_reservations(id TEXT PRIMARY KEY,class TEXT NOT NULL,pool TEXT NOT NULL,bytes INTEGER NOT NULL CHECK(bytes>0),
 owner TEXT NOT NULL,generation INTEGER NOT NULL,expires_at INTEGER NOT NULL,state TEXT NOT NULL CHECK(state IN('RESERVED','COMMITTED','SETTLED','RELEASED')),
 token TEXT NOT NULL DEFAULT (lower(hex(randomblob(16)))));
CREATE INDEX pa_reservation_expiry ON pa_reservations(state,expires_at);
CREATE TABLE pa_scopes(id TEXT PRIMARY KEY,cohort TEXT NOT NULL,revision INTEGER NOT NULL DEFAULT 0,
 pending INTEGER NOT NULL DEFAULT 0 CHECK(pending>=0),quarantine INTEGER NOT NULL DEFAULT 0,conflict INTEGER NOT NULL DEFAULT 0,
 corrections INTEGER NOT NULL DEFAULT 0 CHECK(corrections>=0),available_at INTEGER,first_available_at INTEGER,first_conflict_at INTEGER,active INTEGER NOT NULL DEFAULT 0,
 hold_until INTEGER NOT NULL DEFAULT 0,original_at INTEGER,kind TEXT NOT NULL DEFAULT 'CANDIDATE',archive_root TEXT,archive_revision INTEGER NOT NULL DEFAULT 0,processing_eligible INTEGER NOT NULL DEFAULT 0,recovery_required INTEGER NOT NULL DEFAULT 0);
CREATE TABLE pa_events(id TEXT PRIMARY KEY,semantic_key TEXT,kind TEXT NOT NULL,digest TEXT NOT NULL,wire_digest TEXT NOT NULL,
 manifest TEXT NOT NULL CHECK(length(CAST(manifest AS BLOB))<=4096),count INTEGER NOT NULL CHECK(count BETWEEN 1 AND 50),total_bytes INTEGER NOT NULL CHECK(total_bytes BETWEEN 1 AND 2097152),
 first_at INTEGER NOT NULL,expires_at INTEGER NOT NULL,state TEXT NOT NULL DEFAULT 'PARTIAL' CHECK(state IN('PARTIAL','STAGED','PENDING','COMMITTED','CONFLICT','EXPIRED','DLQ')),
 attempts INTEGER NOT NULL DEFAULT 0,replays INTEGER NOT NULL DEFAULT 0,terminal_capture_generation INTEGER NOT NULL DEFAULT -1,terminal_archive_generation INTEGER NOT NULL DEFAULT -1,next_at INTEGER NOT NULL DEFAULT 0,
 first_failure INTEGER,last_failure INTEGER,reason TEXT,classification TEXT,available_at INTEGER,published_at INTEGER,reservation TEXT NOT NULL REFERENCES pa_reservations(id));
CREATE INDEX pa_event_work ON pa_events(state,next_at,expires_at);
CREATE TABLE pa_fragments(event_id TEXT NOT NULL REFERENCES pa_events(id) ON DELETE CASCADE,ordinal INTEGER NOT NULL,
 digest TEXT NOT NULL,wire TEXT NOT NULL CHECK(length(CAST(wire AS BLOB))<=60000),PRIMARY KEY(event_id,ordinal));
CREATE TABLE pa_ingress(event_id TEXT PRIMARY KEY REFERENCES pa_events(id) ON DELETE CASCADE,state TEXT NOT NULL CHECK(state IN('PENDING','COMMITTED','CONFLICT','ABANDONED')),registered_at INTEGER NOT NULL);
CREATE TABLE pa_affected(event_id TEXT NOT NULL REFERENCES pa_ingress(event_id) ON DELETE CASCADE,scope_id TEXT NOT NULL REFERENCES pa_scopes(id),PRIMARY KEY(event_id,scope_id));
CREATE TABLE pa_records(id TEXT PRIMARY KEY,semantic_key TEXT NOT NULL UNIQUE,kind TEXT NOT NULL,scope_id TEXT,cohort TEXT NOT NULL,
 digest TEXT NOT NULL,bytes INTEGER NOT NULL CHECK(bytes BETWEEN 1 AND 2097152),created_at INTEGER NOT NULL,available_at INTEGER NOT NULL,
 revision INTEGER NOT NULL,definition TEXT NOT NULL,ready INTEGER NOT NULL DEFAULT 0,chunk_count INTEGER NOT NULL CHECK(chunk_count BETWEEN 1 AND 50),location TEXT NOT NULL DEFAULT 'LIVE' CHECK(location IN('LIVE','ARCHIVED')),
 archive_key TEXT,retention INTEGER NOT NULL,category TEXT NOT NULL,order_key TEXT NOT NULL,codec TEXT NOT NULL,encoded_bytes INTEGER NOT NULL);
CREATE INDEX pa_record_report ON pa_records(cohort,available_at,semantic_key);
CREATE INDEX pa_record_order ON pa_records(cohort,definition,order_key);
CREATE INDEX pa_record_scope ON pa_records(scope_id,revision);
CREATE TABLE pa_chunks(record_id TEXT NOT NULL REFERENCES pa_records(id) ON DELETE CASCADE,ordinal INTEGER NOT NULL,
 data TEXT NOT NULL CHECK(length(CAST(data AS BLOB))<=60000),PRIMARY KEY(record_id,ordinal));
CREATE TABLE pa_dependencies(owner_id TEXT NOT NULL REFERENCES pa_records(id) ON DELETE CASCADE,dependency TEXT NOT NULL,PRIMARY KEY(owner_id,dependency));
CREATE TABLE pa_bindings(evaluation_id TEXT PRIMARY KEY,digest TEXT NOT NULL,signal_id TEXT,first_event TEXT NOT NULL,cohort TEXT NOT NULL,cycle_key TEXT);
CREATE INDEX pa_binding_signal ON pa_bindings(signal_id,evaluation_id);
CREATE TABLE pa_projections(scope_id TEXT PRIMARY KEY REFERENCES pa_scopes(id),epoch TEXT NOT NULL,source_revision INTEGER NOT NULL,
 digest TEXT NOT NULL,payload TEXT NOT NULL CHECK(length(CAST(payload AS BLOB))<=60000),available_at INTEGER NOT NULL,
 integrity TEXT NOT NULL CHECK(integrity IN('COMPLETE','PARTIAL','CONFLICT')),fact_identity TEXT NOT NULL,fact_count INTEGER NOT NULL);
CREATE TABLE pa_jobs(id TEXT PRIMARY KEY,class TEXT NOT NULL,scope_id TEXT,event_id TEXT,state TEXT NOT NULL DEFAULT 'PENDING',
 next_at INTEGER NOT NULL,deadline INTEGER NOT NULL,owner TEXT,generation INTEGER NOT NULL DEFAULT 0,lease_until INTEGER NOT NULL DEFAULT 0,attempts INTEGER NOT NULL DEFAULT 0,lease_nonce TEXT NOT NULL DEFAULT(lower(hex(randomblob(16)))));
CREATE INDEX pa_job_schedule ON pa_jobs(state,class,next_at,deadline);
CREATE TABLE pa_publications(id TEXT PRIMARY KEY,scope_id TEXT NOT NULL REFERENCES pa_scopes(id),epoch TEXT NOT NULL,revision INTEGER NOT NULL,
 state TEXT NOT NULL CHECK(state IN('PLANNED','SNAPSHOT_PINNED','OBJECTS_WRITTEN','VERIFIED','CATALOGUED','EVICTION_ELIGIBLE','STALE')),
 owner TEXT NOT NULL,generation INTEGER NOT NULL,lease_until INTEGER NOT NULL,snapshot TEXT,manifest_key TEXT,receipt_key TEXT,root_key TEXT,
 catalogue_generation INTEGER,parity_verified INTEGER NOT NULL DEFAULT 0,reservation TEXT NOT NULL REFERENCES pa_reservations(id),input_revision INTEGER NOT NULL,domain TEXT NOT NULL DEFAULT 'EVIDENCE' CHECK(domain IN('EVIDENCE','CENSUS')),catalogue_index_root TEXT,catalogue_offset INTEGER NOT NULL DEFAULT 0,sample_offset INTEGER NOT NULL DEFAULT 0,lease_nonce TEXT NOT NULL DEFAULT(lower(hex(randomblob(16)))));
CREATE INDEX pa_publication_scope ON pa_publications(scope_id,state);
CREATE TABLE pa_corrections(id TEXT PRIMARY KEY,scope_id TEXT NOT NULL REFERENCES pa_scopes(id),original_at INTEGER NOT NULL,admitted_at INTEGER NOT NULL,
 predecessor TEXT NOT NULL,reason TEXT NOT NULL,dependencies TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN('PENDING','CONFLICT','COMPLETE','UNREDUCIBLE','LATE_NOT_ADMITTED','EVIDENCE_UNAVAILABLE')),
 result_id TEXT,deadline INTEGER NOT NULL,reservation TEXT NOT NULL REFERENCES pa_reservations(id),expected_revision INTEGER NOT NULL,predecessor_digest TEXT NOT NULL,initial_state TEXT NOT NULL);
CREATE INDEX pa_correction_predecessor ON pa_corrections(scope_id,predecessor);
CREATE TABLE pa_incidents(id TEXT PRIMARY KEY,category TEXT NOT NULL,first_at INTEGER NOT NULL,last_at INTEGER NOT NULL,state TEXT NOT NULL,
 payload TEXT NOT NULL CHECK(length(CAST(payload AS BLOB))<=8192),scope_id TEXT,reservation TEXT NOT NULL REFERENCES pa_reservations(id));
CREATE TABLE pa_coverage(id TEXT PRIMARY KEY,cohort TEXT NOT NULL,from_at INTEGER NOT NULL,to_at INTEGER NOT NULL,status TEXT NOT NULL CHECK(status IN('COMPLETE_FOR_SCOPE','KNOWN_GAPS','UNKNOWN_CAPTURE')),
 known_missing INTEGER,record_id TEXT NOT NULL REFERENCES pa_records(id));
CREATE TABLE pa_pins(id TEXT PRIMARY KEY,scope_id TEXT NOT NULL,kind TEXT NOT NULL,owner TEXT NOT NULL,generation INTEGER NOT NULL,until_at INTEGER NOT NULL);
CREATE TABLE pa_effects(id TEXT PRIMARY KEY,scope_id TEXT NOT NULL,epoch TEXT NOT NULL,revision INTEGER NOT NULL,payload TEXT NOT NULL CHECK(length(CAST(payload AS BLOB))<=60000),created_at INTEGER NOT NULL);
CREATE TABLE pa_restores(id TEXT PRIMARY KEY,root_key TEXT NOT NULL,scope_id TEXT,offset INTEGER NOT NULL DEFAULT 0,state TEXT NOT NULL,
 owner TEXT NOT NULL,generation INTEGER NOT NULL,lease_until INTEGER NOT NULL,restored_at INTEGER NOT NULL,source_epoch TEXT NOT NULL,reservation TEXT NOT NULL REFERENCES pa_reservations(id),lease_nonce TEXT NOT NULL DEFAULT(lower(hex(randomblob(16)))));
CREATE TABLE pa_format_work(id TEXT PRIMARY KEY,old_root TEXT NOT NULL,state TEXT NOT NULL,new_manifest TEXT,new_receipt TEXT,new_root TEXT);
CREATE TABLE pa_cohort_state(cohort TEXT PRIMARY KEY,revision INTEGER NOT NULL,pending INTEGER NOT NULL DEFAULT 0 CHECK(pending>=0),conflict INTEGER NOT NULL DEFAULT 0);
CREATE TABLE pa_samples(id TEXT PRIMARY KEY,digest TEXT NOT NULL,record_id TEXT NOT NULL,cohort TEXT NOT NULL,available_at INTEGER NOT NULL);
CREATE INDEX pa_sample_record ON pa_samples(record_id);
CREATE TABLE pa_processing(scope_id TEXT PRIMARY KEY REFERENCES pa_scopes(id),revision INTEGER NOT NULL,market_revision INTEGER NOT NULL,payload TEXT NOT NULL CHECK(length(CAST(payload AS BLOB))<=60000),updated_at INTEGER NOT NULL);
CREATE TABLE pa_environment(singleton INTEGER PRIMARY KEY CHECK(singleton=1),plane TEXT NOT NULL,environment TEXT NOT NULL,resource_id TEXT NOT NULL);
INSERT INTO pa_environment VALUES(1,'MEASUREMENT','synthetic-b1-package-a','LOCAL_ONLY_NO_CLOUD_ID');
CREATE TABLE pa_market_ingress(id TEXT PRIMARY KEY,scope_id TEXT NOT NULL,cohort TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN('PENDING','COMMITTED','CONFLICT','ABANDONED')));
CREATE TABLE pa_semantic_ingress(event_id TEXT PRIMARY KEY,semantic_key TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN('PENDING','COMMITTED','CONFLICT','ABANDONED')));
CREATE TABLE pa_semantic_affected(event_id TEXT NOT NULL REFERENCES pa_semantic_ingress(event_id) ON DELETE CASCADE,scope_id TEXT NOT NULL REFERENCES pa_scopes(id),PRIMARY KEY(event_id,scope_id));
CREATE TRIGGER pa_semantic_pending AFTER INSERT ON pa_semantic_affected BEGIN UPDATE pa_scopes SET revision=revision+1,pending=pending+1 WHERE id=NEW.scope_id; UPDATE pa_meta SET revision=revision+1; END;
CREATE TRIGGER pa_semantic_finished AFTER UPDATE OF state ON pa_semantic_ingress WHEN OLD.state='PENDING' AND NEW.state!='PENDING' BEGIN UPDATE pa_scopes SET revision=revision+1,pending=pending-1 WHERE id IN(SELECT scope_id FROM pa_semantic_affected WHERE event_id=NEW.event_id); UPDATE pa_meta SET revision=revision+1; END;
CREATE TRIGGER pa_market_pending AFTER INSERT ON pa_market_ingress BEGIN UPDATE pa_cohort_state SET revision=revision+1,pending=pending+1 WHERE cohort=NEW.cohort; UPDATE pa_meta SET revision=revision+1; END;
CREATE TRIGGER pa_market_finished AFTER UPDATE OF state ON pa_market_ingress WHEN OLD.state='PENDING' AND NEW.state!='PENDING' BEGIN UPDATE pa_cohort_state SET revision=revision+1,pending=pending-1,conflict=MAX(conflict,NEW.state='CONFLICT') WHERE cohort=NEW.cohort; UPDATE pa_meta SET revision=revision+1; END;
-- Durable revisions cover membership AND absence. No process-local fence is authoritative.
CREATE TRIGGER pa_affected_insert AFTER INSERT ON pa_affected BEGIN UPDATE pa_scopes SET revision=revision+1,pending=pending+1 WHERE id=NEW.scope_id; UPDATE pa_meta SET revision=revision+1; END;
CREATE TRIGGER pa_ingress_finish AFTER UPDATE OF state ON pa_ingress WHEN OLD.state='PENDING' AND NEW.state!='PENDING' BEGIN
 UPDATE pa_scopes SET revision=revision+1,pending=pending-1 WHERE id IN(SELECT scope_id FROM pa_affected WHERE event_id=NEW.event_id); UPDATE pa_meta SET revision=revision+1; END;
CREATE TRIGGER pa_binding_revision AFTER INSERT ON pa_bindings BEGIN UPDATE pa_scopes SET revision=revision+1 WHERE id=NEW.evaluation_id OR id=NEW.signal_id; UPDATE pa_meta SET revision=revision+1; END;
CREATE TRIGGER pa_record_revision AFTER INSERT ON pa_records WHEN NEW.ready=1 BEGIN UPDATE pa_scopes SET revision=revision+1,available_at=MAX(COALESCE(available_at,0),NEW.available_at),first_available_at=MIN(COALESCE(first_available_at,NEW.available_at),NEW.available_at),first_conflict_at=CASE WHEN NEW.kind='CONFLICT' THEN MIN(COALESCE(first_conflict_at,NEW.available_at),NEW.available_at) ELSE first_conflict_at END WHERE (id=NEW.scope_id OR id IN(SELECT signal_id FROM pa_bindings WHERE evaluation_id=NEW.scope_id) OR id IN(SELECT evaluation_id FROM pa_bindings WHERE signal_id=NEW.scope_id)) AND NEW.kind!='RESULT'; UPDATE pa_meta SET revision=revision+1; END;
CREATE TRIGGER pa_record_available AFTER UPDATE OF ready ON pa_records WHEN OLD.ready=0 AND NEW.ready=1 BEGIN UPDATE pa_scopes SET revision=revision+1,available_at=MAX(COALESCE(available_at,0),NEW.available_at),first_available_at=MIN(COALESCE(first_available_at,NEW.available_at),NEW.available_at),first_conflict_at=CASE WHEN NEW.kind='CONFLICT' THEN MIN(COALESCE(first_conflict_at,NEW.available_at),NEW.available_at) ELSE first_conflict_at END WHERE (id=NEW.scope_id OR id IN(SELECT signal_id FROM pa_bindings WHERE evaluation_id=NEW.scope_id) OR id IN(SELECT evaluation_id FROM pa_bindings WHERE signal_id=NEW.scope_id)) AND NEW.kind!='RESULT'; UPDATE pa_meta SET revision=revision+1; END;
CREATE TRIGGER pa_record_immutable BEFORE UPDATE OF id,semantic_key,kind,scope_id,cohort,digest,bytes,created_at,available_at,revision,definition,retention,category,order_key,codec,encoded_bytes,chunk_count ON pa_records WHEN OLD.ready=1 BEGIN SELECT RAISE(ABORT,'pa_record_immutable'); END;
CREATE TRIGGER pa_chunk_immutable BEFORE UPDATE ON pa_chunks BEGIN SELECT RAISE(ABORT,'pa_chunk_immutable'); END;
CREATE TABLE pa_pool_use(pool TEXT PRIMARY KEY,charged INTEGER NOT NULL DEFAULT 0 CHECK(charged>=0));
INSERT INTO pa_pool_use(pool) VALUES('normal'),('incidents'),('catchup'),('archive'),('restore');
CREATE TRIGGER pa_binding_immutable BEFORE UPDATE ON pa_bindings BEGIN SELECT RAISE(ABORT,'pa_binding_immutable'); END;
CREATE TRIGGER pa_projection_fence BEFORE INSERT ON pa_projections WHEN NOT EXISTS(SELECT 1 FROM pa_scopes s,pa_meta m WHERE s.id=NEW.scope_id AND m.epoch=NEW.epoch AND s.revision=NEW.source_revision AND s.pending=0 AND s.corrections=0 AND (NEW.integrity!='COMPLETE' OR (s.quarantine=0 AND s.conflict=0))) BEGIN SELECT RAISE(ABORT,'pa_projection_fence'); END;
CREATE TRIGGER pa_projection_update_fence BEFORE UPDATE ON pa_projections WHEN NOT EXISTS(SELECT 1 FROM pa_scopes s,pa_meta m WHERE s.id=NEW.scope_id AND m.epoch=NEW.epoch AND s.revision=NEW.source_revision AND s.pending=0 AND s.corrections=0 AND (NEW.integrity!='COMPLETE' OR (s.quarantine=0 AND s.conflict=0))) BEGIN SELECT RAISE(ABORT,'pa_projection_fence'); END;
CREATE TRIGGER pa_scope_state_revision AFTER UPDATE OF recovery_required,pending,quarantine,conflict,corrections,active,hold_until ON pa_scopes WHEN NEW.recovery_required!=OLD.recovery_required OR NEW.pending!=OLD.pending OR NEW.quarantine!=OLD.quarantine OR NEW.conflict!=OLD.conflict OR NEW.corrections!=OLD.corrections OR NEW.active!=OLD.active OR NEW.hold_until!=OLD.hold_until BEGIN UPDATE pa_scopes SET revision=revision+1 WHERE id=NEW.id OR id IN(SELECT signal_id FROM pa_bindings WHERE evaluation_id=NEW.id) OR id IN(SELECT evaluation_id FROM pa_bindings WHERE signal_id=NEW.id); UPDATE pa_meta SET revision=revision+1; END;

-- Hard live-cardinality backstops supplement byte reservations. Duplicate identity
-- remains admissible at the limit; new work fails before partial writes.
CREATE TRIGGER pa_jobs_capacity BEFORE INSERT ON pa_jobs WHEN NOT EXISTS(SELECT 1 FROM pa_jobs WHERE id=NEW.id) AND (SELECT COUNT(*) FROM pa_jobs)>=8192 BEGIN SELECT RAISE(ABORT,'pa_live_capacity'); END;
CREATE TRIGGER pa_records_capacity BEFORE INSERT ON pa_records WHEN NOT EXISTS(SELECT 1 FROM pa_records WHERE id=NEW.id) AND (SELECT COUNT(*) FROM pa_records)>=65536 BEGIN SELECT RAISE(ABORT,'pa_live_capacity'); END;
CREATE TRIGGER pa_bindings_capacity BEFORE INSERT ON pa_bindings WHEN NOT EXISTS(SELECT 1 FROM pa_bindings WHERE evaluation_id=NEW.evaluation_id) AND (SELECT COUNT(*) FROM pa_bindings)>=4032 BEGIN SELECT RAISE(ABORT,'pa_live_capacity'); END;
CREATE TRIGGER pa_cohort_state_capacity BEFORE INSERT ON pa_cohort_state WHEN NOT EXISTS(SELECT 1 FROM pa_cohort_state WHERE cohort=NEW.cohort) AND (SELECT COUNT(*) FROM pa_cohort_state)>=256 BEGIN SELECT RAISE(ABORT,'pa_live_capacity'); END;
CREATE TRIGGER pa_events_capacity BEFORE INSERT ON pa_events WHEN NOT EXISTS(SELECT 1 FROM pa_events WHERE id=NEW.id) AND (SELECT COUNT(*) FROM pa_events)>=1024 BEGIN SELECT RAISE(ABORT,'pa_live_capacity'); END;
CREATE TRIGGER pa_reservations_capacity BEFORE INSERT ON pa_reservations WHEN NOT EXISTS(SELECT 1 FROM pa_reservations WHERE id=NEW.id) AND (SELECT COUNT(*) FROM pa_reservations)>=8192 BEGIN SELECT RAISE(ABORT,'pa_live_capacity'); END;
CREATE TRIGGER pa_publications_capacity BEFORE INSERT ON pa_publications WHEN NOT EXISTS(SELECT 1 FROM pa_publications WHERE id=NEW.id) AND (SELECT COUNT(*) FROM pa_publications)>=64 BEGIN SELECT RAISE(ABORT,'pa_live_capacity'); END;
CREATE TRIGGER pa_incidents_capacity BEFORE INSERT ON pa_incidents WHEN NOT EXISTS(SELECT 1 FROM pa_incidents WHERE id=NEW.id) AND (SELECT COUNT(*) FROM pa_incidents)>=256 BEGIN SELECT RAISE(ABORT,'pa_live_capacity'); END;
CREATE TRIGGER pa_corrections_capacity BEFORE INSERT ON pa_corrections WHEN NOT EXISTS(SELECT 1 FROM pa_corrections WHERE id=NEW.id) AND (SELECT COUNT(*) FROM pa_corrections)>=256 BEGIN SELECT RAISE(ABORT,'pa_live_capacity'); END;
CREATE TRIGGER pa_restores_capacity BEFORE INSERT ON pa_restores WHEN NOT EXISTS(SELECT 1 FROM pa_restores WHERE id=NEW.id) AND (SELECT COUNT(*) FROM pa_restores)>=64 BEGIN SELECT RAISE(ABORT,'pa_live_capacity'); END;
CREATE TRIGGER pa_format_work_capacity BEFORE INSERT ON pa_format_work WHEN NOT EXISTS(SELECT 1 FROM pa_format_work WHERE id=NEW.id) AND (SELECT COUNT(*) FROM pa_format_work)>=64 BEGIN SELECT RAISE(ABORT,'pa_live_capacity'); END;
CREATE TRIGGER pa_pins_capacity BEFORE INSERT ON pa_pins WHEN NOT EXISTS(SELECT 1 FROM pa_pins WHERE id=NEW.id) AND (SELECT COUNT(*) FROM pa_pins)>=8192 BEGIN SELECT RAISE(ABORT,'pa_live_capacity'); END;
CREATE TRIGGER pa_effects_capacity BEFORE INSERT ON pa_effects WHEN NOT EXISTS(SELECT 1 FROM pa_effects WHERE id=NEW.id) AND (SELECT COUNT(*) FROM pa_effects)>=8192 BEGIN SELECT RAISE(ABORT,'pa_live_capacity'); END;
CREATE TRIGGER pa_coverage_capacity BEFORE INSERT ON pa_coverage WHEN NOT EXISTS(SELECT 1 FROM pa_coverage WHERE id=NEW.id) AND (SELECT COUNT(*) FROM pa_coverage)>=1024 BEGIN SELECT RAISE(ABORT,'pa_live_capacity'); END;
CREATE TRIGGER pa_projections_capacity BEFORE INSERT ON pa_projections WHEN NOT EXISTS(SELECT 1 FROM pa_projections WHERE scope_id=NEW.scope_id) AND (SELECT COUNT(*) FROM pa_projections)>=4032 BEGIN SELECT RAISE(ABORT,'pa_live_capacity'); END;
CREATE TRIGGER pa_processing_capacity BEFORE INSERT ON pa_processing WHEN NOT EXISTS(SELECT 1 FROM pa_processing WHERE scope_id=NEW.scope_id) AND (SELECT COUNT(*) FROM pa_processing)>=4032 BEGIN SELECT RAISE(ABORT,'pa_live_capacity'); END;
CREATE TRIGGER pa_semantic_ingress_capacity BEFORE INSERT ON pa_semantic_ingress WHEN NOT EXISTS(SELECT 1 FROM pa_semantic_ingress WHERE event_id=NEW.event_id) AND (SELECT COUNT(*) FROM pa_semantic_ingress)>=2048 BEGIN SELECT RAISE(ABORT,'pa_live_capacity'); END;
CREATE TRIGGER pa_ingress_capacity BEFORE INSERT ON pa_ingress WHEN NOT EXISTS(SELECT 1 FROM pa_ingress WHERE event_id=NEW.event_id) AND (SELECT COUNT(*) FROM pa_ingress)>=1024 BEGIN SELECT RAISE(ABORT,'pa_live_capacity'); END;
CREATE TRIGGER pa_samples_capacity BEFORE INSERT ON pa_samples WHEN NOT EXISTS(SELECT 1 FROM pa_samples WHERE id=NEW.id) AND (SELECT COUNT(*) FROM pa_samples)>=16384 BEGIN SELECT RAISE(ABORT,'pa_live_capacity'); END;
CREATE TRIGGER pa_market_ingress_capacity BEFORE INSERT ON pa_market_ingress WHEN NOT EXISTS(SELECT 1 FROM pa_market_ingress WHERE id=NEW.id) AND (SELECT COUNT(*) FROM pa_market_ingress)>=1024 BEGIN SELECT RAISE(ABORT,'pa_live_capacity'); END;
CREATE TRIGGER pa_scopes_capacity BEFORE INSERT ON pa_scopes WHEN NOT EXISTS(SELECT 1 FROM pa_scopes WHERE id=NEW.id) AND (SELECT COUNT(*) FROM pa_scopes)>=4032 BEGIN SELECT RAISE(ABORT,'pa_live_capacity'); END;
CREATE TRIGGER pa_dependency_capacity BEFORE INSERT ON pa_dependencies WHEN NOT EXISTS(SELECT 1 FROM pa_dependencies WHERE owner_id=NEW.owner_id AND dependency=NEW.dependency) AND (SELECT COUNT(*) FROM pa_dependencies WHERE owner_id=NEW.owner_id)>=128 BEGIN SELECT RAISE(ABORT,'pa_dependency_capacity'); END;

CREATE TRIGGER pa_binding_removed AFTER DELETE ON pa_bindings BEGIN UPDATE pa_scopes SET revision=revision+1 WHERE id=OLD.evaluation_id OR id=OLD.signal_id; UPDATE pa_meta SET revision=revision+1; END;

CREATE TRIGGER pa_archive_membership_insert AFTER INSERT ON pa_records WHEN NEW.ready=1 BEGIN UPDATE pa_scopes SET archive_revision=archive_revision+1 WHERE id=NEW.scope_id; END;
CREATE TRIGGER pa_archive_membership_ready AFTER UPDATE OF ready ON pa_records WHEN OLD.ready=0 AND NEW.ready=1 BEGIN UPDATE pa_scopes SET archive_revision=archive_revision+1 WHERE id=NEW.scope_id; END;
CREATE TRIGGER pa_archive_membership_delete AFTER DELETE ON pa_records WHEN OLD.ready=1 BEGIN UPDATE pa_scopes SET archive_revision=archive_revision+1 WHERE id=OLD.scope_id; END;

-- Future qualified allocation evidence is ADMIN-installed only, absent by default.
CREATE TABLE pa_resource_qualification(singleton INTEGER PRIMARY KEY CHECK(singleton=1),resource_id TEXT NOT NULL,qualification_digest TEXT NOT NULL,size_after_conservative INTEGER NOT NULL CHECK(size_after_conservative=1),reuse_verified INTEGER NOT NULL DEFAULT 0);
CREATE TRIGGER pa_reservation_resource_insert AFTER INSERT ON pa_reservations BEGIN UPDATE pa_meta SET resource_revision=resource_revision+1; END;
CREATE TRIGGER pa_reservation_resource_state AFTER UPDATE OF state ON pa_reservations WHEN NEW.state!=OLD.state BEGIN UPDATE pa_meta SET resource_revision=resource_revision+1; END;
CREATE TRIGGER pa_reservation_resource_delete AFTER DELETE ON pa_reservations BEGIN UPDATE pa_meta SET resource_revision=resource_revision+1; END;
CREATE TRIGGER pa_physical_resource_revision AFTER UPDATE OF physical_charged ON pa_meta WHEN NEW.physical_charged!=OLD.physical_charged BEGIN UPDATE pa_meta SET resource_revision=resource_revision+1; END;
CREATE TRIGGER pa_logical_delete_observation AFTER DELETE ON pa_records BEGIN UPDATE pa_meta SET deletion_generation=deletion_generation+1,resource_revision=resource_revision+1; END;
