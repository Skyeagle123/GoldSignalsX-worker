-- Local measurement plane only: per-subject publication/collection fences.
CREATE TABLE measurement_lifecycle_revisions (
 signal_id TEXT PRIMARY KEY,
 revision INTEGER NOT NULL CHECK(revision>=0)
);
INSERT INTO measurement_lifecycle_revisions SELECT signal_id,1 FROM measurement_lifecycle_facts WHERE signal_id IS NOT NULL GROUP BY signal_id;
ALTER TABLE measurement_lifecycle_projection ADD COLUMN source_revision INTEGER NOT NULL DEFAULT 0;
CREATE TABLE measurement_decision_quarantine (
 evaluation_id TEXT PRIMARY KEY REFERENCES measurement_decision_bindings(evaluation_id),
 state TEXT NOT NULL CHECK(state IN ('PENDING','CONFLICT')),
 detected_at INTEGER NOT NULL CHECK(detected_at>=0)
);
INSERT INTO measurement_decision_quarantine SELECT evaluation_id,'CONFLICT',MIN(detected_at) FROM measurement_decision_conflicts GROUP BY evaluation_id;
CREATE TRIGGER measurement_fact_revision AFTER INSERT ON measurement_lifecycle_facts WHEN NEW.signal_id IS NOT NULL
 BEGIN INSERT INTO measurement_lifecycle_revisions VALUES(NEW.signal_id,1) ON CONFLICT(signal_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER measurement_availability_revision AFTER UPDATE OF ingested_at ON measurement_ingress_recovery
 WHEN OLD.ingested_at IS NULL AND NEW.ingested_at IS NOT NULL
 BEGIN INSERT INTO measurement_lifecycle_revisions SELECT signal_id,1 FROM measurement_lifecycle_facts WHERE event_id=NEW.event_id AND signal_id IS NOT NULL
 ON CONFLICT(signal_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER measurement_quarantine_revision AFTER INSERT ON measurement_decision_quarantine
 BEGIN
 INSERT INTO measurement_lifecycle_revisions SELECT official_signal_id,1 FROM measurement_decision_bindings WHERE evaluation_id=NEW.evaluation_id AND official_signal_id IS NOT NULL
 ON CONFLICT(signal_id) DO UPDATE SET revision=revision+1;
 UPDATE measurement_lifecycle_projection SET integrity_status='CONFLICT',status=NULL,closed_at=NULL WHERE signal_id=(SELECT official_signal_id FROM measurement_decision_bindings WHERE evaluation_id=NEW.evaluation_id);
 END;
CREATE TRIGGER measurement_quarantine_changed AFTER UPDATE OF state ON measurement_decision_quarantine WHEN OLD.state!=NEW.state
 BEGIN
 UPDATE measurement_lifecycle_revisions SET revision=revision+1 WHERE signal_id=(SELECT official_signal_id FROM measurement_decision_bindings WHERE evaluation_id=NEW.evaluation_id);
 UPDATE measurement_lifecycle_projection SET integrity_status='CONFLICT',status=NULL,closed_at=NULL WHERE signal_id=(SELECT official_signal_id FROM measurement_decision_bindings WHERE evaluation_id=NEW.evaluation_id);
 END;
CREATE TRIGGER measurement_quarantine_no_delete BEFORE DELETE ON measurement_decision_quarantine BEGIN SELECT RAISE(ABORT,'measurement_quarantine_immutable'); END;
CREATE TRIGGER measurement_quarantine_no_downgrade BEFORE UPDATE ON measurement_decision_quarantine WHEN OLD.state='CONFLICT' AND NEW.state!='CONFLICT'
 BEGIN SELECT RAISE(ABORT,'measurement_quarantine_immutable'); END;
CREATE TRIGGER measurement_conflict_quarantine AFTER INSERT ON measurement_decision_conflicts
 BEGIN INSERT INTO measurement_decision_quarantine VALUES(NEW.evaluation_id,'CONFLICT',NEW.detected_at)
 ON CONFLICT(evaluation_id) DO UPDATE SET state='CONFLICT'; END;

-- Existing projections deliberately remain revision 0 until explicit rebuild.
CREATE TRIGGER measurement_projection_insert_fence BEFORE INSERT ON measurement_lifecycle_projection
 WHEN NEW.source_revision!=COALESCE((SELECT revision FROM measurement_lifecycle_revisions WHERE signal_id=NEW.signal_id),0)
 OR (NEW.integrity_status='COMPLETE' AND EXISTS(SELECT 1 FROM measurement_decision_quarantine q JOIN measurement_decision_bindings b USING(evaluation_id) WHERE b.official_signal_id=NEW.signal_id))
 BEGIN SELECT RAISE(ABORT,'measurement_projection_rebuild_raced'); END;
CREATE TRIGGER measurement_projection_update_fence BEFORE UPDATE OF source_revision,payload_blob ON measurement_lifecycle_projection
 WHEN NEW.source_revision!=COALESCE((SELECT revision FROM measurement_lifecycle_revisions WHERE signal_id=NEW.signal_id),0)
 OR (NEW.integrity_status='COMPLETE' AND EXISTS(SELECT 1 FROM measurement_decision_quarantine q JOIN measurement_decision_bindings b USING(evaluation_id) WHERE b.official_signal_id=NEW.signal_id))
 BEGIN SELECT RAISE(ABORT,'measurement_projection_rebuild_raced'); END;

-- A guard row exists only inside a writer batch and is deleted before commit.
-- Raising here rolls back the entire associated write, rather than merely
-- returning zero changed rows after irreversible evidence was committed.
CREATE TABLE measurement_collector_guards (
 signal_id TEXT PRIMARY KEY,
 revision INTEGER NOT NULL,
 as_of INTEGER NOT NULL
);
CREATE TRIGGER measurement_collector_commit_fence BEFORE INSERT ON measurement_collector_guards
 WHEN NOT EXISTS(SELECT 1 FROM measurement_lifecycle_projection p JOIN measurement_lifecycle_revisions v USING(signal_id)
 WHERE p.signal_id=NEW.signal_id AND v.revision=NEW.revision AND p.source_revision=v.revision
 AND p.integrity_status='COMPLETE' AND p.available_at<=NEW.as_of
 AND p.fact_count=(SELECT COUNT(*) FROM measurement_lifecycle_facts WHERE signal_id=p.signal_id)
 AND NOT EXISTS(SELECT 1 FROM measurement_lifecycle_facts f LEFT JOIN measurement_ingress_recovery a USING(event_id) WHERE f.signal_id=p.signal_id AND a.ingested_at IS NULL)
 AND p.available_at=(SELECT MAX(a.ingested_at) FROM measurement_lifecycle_facts f JOIN measurement_ingress_recovery a USING(event_id) WHERE f.signal_id=p.signal_id)
 AND NOT EXISTS(SELECT 1 FROM measurement_decision_quarantine q JOIN measurement_decision_bindings b USING(evaluation_id) WHERE b.official_signal_id=p.signal_id))
 BEGIN SELECT RAISE(ABORT,'measurement_lifecycle_commit_changed'); END;
