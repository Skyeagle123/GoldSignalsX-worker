-- Additive local B1 remediation. Apply explicitly only; no request-time repair.
-- Signed addresses: compact record_id >0; legacy rowid <0; zero marks an
-- atomically validated dependency-free owner. Relationship bits preserve roles.
CREATE TABLE measurement_outcome_dependencies (
 owner_ref INTEGER NOT NULL, target_ref INTEGER NOT NULL, relations INTEGER NOT NULL CHECK((target_ref=0 AND relations=64) OR (target_ref!=0 AND relations BETWEEN 1 AND 63)),
 CHECK(owner_ref!=0),
 PRIMARY KEY(owner_ref,target_ref)
) WITHOUT ROWID;
CREATE INDEX measurement_outcome_dependency_target ON measurement_outcome_dependencies(target_ref,owner_ref);
CREATE TRIGGER measurement_dependency_compact_cleanup AFTER DELETE ON measurement_outcome_records BEGIN
 DELETE FROM measurement_outcome_dependencies WHERE owner_ref=OLD.record_id;
END;
CREATE TRIGGER measurement_dependency_legacy_cleanup AFTER DELETE ON measurement_legacy_outcome_evidence BEGIN
 DELETE FROM measurement_outcome_dependencies WHERE owner_ref=-OLD.rowid;
END;
CREATE TRIGGER measurement_dependency_target_exists BEFORE INSERT ON measurement_outcome_dependencies
 WHEN ((NEW.owner_ref>0 AND NOT EXISTS(SELECT 1 FROM measurement_outcome_records r WHERE r.record_id=NEW.owner_ref)) OR (NEW.owner_ref<0 AND NOT EXISTS(SELECT 1 FROM measurement_legacy_outcome_evidence r WHERE r.rowid=-NEW.owner_ref))) OR (NEW.target_ref!=0 AND ((NEW.target_ref>0 AND NOT EXISTS(SELECT 1 FROM measurement_outcome_records r WHERE r.record_id=NEW.target_ref)) OR (NEW.target_ref<0 AND NOT EXISTS(SELECT 1 FROM measurement_legacy_outcome_evidence r WHERE r.rowid=-NEW.target_ref)))) BEGIN
 SELECT RAISE(ABORT,'measurement_evidence_reference_missing');
END;
CREATE TRIGGER measurement_compact_dependency_retained BEFORE DELETE ON measurement_outcome_records
 WHEN EXISTS(SELECT 1 FROM measurement_outcome_dependencies x WHERE x.target_ref=OLD.record_id) BEGIN
 SELECT RAISE(ABORT,'measurement_dependency_retained');
END;
CREATE TRIGGER measurement_legacy_dependency_retained BEFORE DELETE ON measurement_legacy_outcome_evidence
 WHEN EXISTS(SELECT 1 FROM measurement_outcome_dependencies x WHERE x.target_ref=-OLD.rowid) BEGIN
 SELECT RAISE(ABORT,'measurement_dependency_retained');
END;
-- Pre-migration compressed evidence is retained until an explicit writer replay
-- validates its dependency graph. GET never backfills or repairs it.
UPDATE measurement_schema_meta SET version=4;
