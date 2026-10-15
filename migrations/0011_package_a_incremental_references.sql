-- Forward-only append-aware reference obligations. No historical edge log.
ALTER TABLE pa_meta ADD COLUMN reference_sequence INTEGER NOT NULL DEFAULT 0 CHECK(reference_sequence BETWEEN 0 AND 9007199254740991);
CREATE VIEW pa_registered_archive_roots AS
SELECT root_key FROM pa_meta WHERE root_key IS NOT NULL
 UNION SELECT archive_root FROM pa_scopes WHERE archive_root IS NOT NULL
 UNION SELECT root_key FROM pa_publications WHERE root_key IS NOT NULL
 UNION SELECT root_key FROM pa_restores WHERE root_key IS NOT NULL
 UNION SELECT source_root FROM pa_archive_lineage
 UNION SELECT old_root FROM pa_format_work WHERE old_root IS NOT NULL
 UNION SELECT new_root FROM pa_format_work WHERE new_root IS NOT NULL;
CREATE TABLE pa_archive_reference_work(
 sequence INTEGER PRIMARY KEY AUTOINCREMENT CHECK(sequence BETWEEN 1 AND 9007199254740991),
 root_key TEXT NOT NULL UNIQUE CHECK(length(CAST(root_key AS BLOB)) BETWEEN 1 AND 1024)
);
CREATE TRIGGER pa_reference_work_capacity BEFORE INSERT ON pa_archive_reference_work
 WHEN (SELECT COUNT(*) FROM pa_archive_reference_work)>=4353
 BEGIN SELECT RAISE(ABORT,'pa_reference_capacity'); END;
-- Independently charge registry row/index/temp overhead atomically. Removal
-- grants no physical or pool credit; qualified physical reconciliation is separate.
CREATE TRIGGER pa_reference_work_charge BEFORE INSERT ON pa_archive_reference_work
 WHEN (SELECT physical_charged FROM pa_meta)+(SELECT COALESCE(SUM(bytes),0) FROM pa_reservations WHERE state='RESERVED')+81920>5500000000
 OR (SELECT charged FROM pa_pool_use WHERE pool='archive')+(SELECT COALESCE(SUM(bytes),0) FROM pa_reservations WHERE pool='archive' AND state='RESERVED')+81920>400000000
 BEGIN SELECT RAISE(ABORT,'pa_reference_resource_capacity'); END;
CREATE TRIGGER pa_reference_work_added AFTER INSERT ON pa_archive_reference_work BEGIN
 UPDATE pa_meta SET reference_sequence=NEW.sequence,physical_charged=physical_charged+81920;
 UPDATE pa_pool_use SET charged=charged+81920 WHERE pool='archive';
END;
CREATE TRIGGER pa_reference_work_immutable BEFORE UPDATE ON pa_archive_reference_work BEGIN SELECT RAISE(ABORT,'pa_reference_immutable'); END;
CREATE TRIGGER pa_reference_work_meta_insert AFTER INSERT ON pa_meta BEGIN
 INSERT INTO pa_archive_reference_work(root_key) SELECT NEW.root_key WHERE NEW.root_key IS NOT NULL AND NOT EXISTS(SELECT 1 FROM pa_archive_reference_work WHERE root_key=NEW.root_key);
END;
CREATE TRIGGER pa_reference_work_meta_update AFTER UPDATE OF root_key ON pa_meta BEGIN
 DELETE FROM pa_archive_reference_work WHERE root_key=OLD.root_key AND NOT EXISTS(SELECT 1 FROM pa_registered_archive_roots r WHERE r.root_key=OLD.root_key);
 INSERT INTO pa_archive_reference_work(root_key) SELECT NEW.root_key WHERE NEW.root_key IS NOT NULL AND NOT EXISTS(SELECT 1 FROM pa_archive_reference_work WHERE root_key=NEW.root_key);
END;
CREATE TRIGGER pa_reference_work_meta_delete AFTER DELETE ON pa_meta BEGIN
 DELETE FROM pa_archive_reference_work WHERE root_key=OLD.root_key AND NOT EXISTS(SELECT 1 FROM pa_registered_archive_roots r WHERE r.root_key=OLD.root_key);
END;
CREATE TRIGGER pa_reference_work_scopes_insert AFTER INSERT ON pa_scopes BEGIN
 INSERT INTO pa_archive_reference_work(root_key) SELECT NEW.archive_root WHERE NEW.archive_root IS NOT NULL AND NOT EXISTS(SELECT 1 FROM pa_archive_reference_work WHERE root_key=NEW.archive_root);
END;
CREATE TRIGGER pa_reference_work_scopes_update AFTER UPDATE OF archive_root ON pa_scopes BEGIN
 DELETE FROM pa_archive_reference_work WHERE root_key=OLD.archive_root AND NOT EXISTS(SELECT 1 FROM pa_registered_archive_roots r WHERE r.root_key=OLD.archive_root);
 INSERT INTO pa_archive_reference_work(root_key) SELECT NEW.archive_root WHERE NEW.archive_root IS NOT NULL AND NOT EXISTS(SELECT 1 FROM pa_archive_reference_work WHERE root_key=NEW.archive_root);
END;
CREATE TRIGGER pa_reference_work_scopes_delete AFTER DELETE ON pa_scopes BEGIN
 DELETE FROM pa_archive_reference_work WHERE root_key=OLD.archive_root AND NOT EXISTS(SELECT 1 FROM pa_registered_archive_roots r WHERE r.root_key=OLD.archive_root);
END;
CREATE TRIGGER pa_reference_work_publications_insert AFTER INSERT ON pa_publications BEGIN
 INSERT INTO pa_archive_reference_work(root_key) SELECT NEW.root_key WHERE NEW.root_key IS NOT NULL AND NOT EXISTS(SELECT 1 FROM pa_archive_reference_work WHERE root_key=NEW.root_key);
END;
CREATE TRIGGER pa_reference_work_publications_update AFTER UPDATE OF root_key ON pa_publications BEGIN
 DELETE FROM pa_archive_reference_work WHERE root_key=OLD.root_key AND NOT EXISTS(SELECT 1 FROM pa_registered_archive_roots r WHERE r.root_key=OLD.root_key);
 INSERT INTO pa_archive_reference_work(root_key) SELECT NEW.root_key WHERE NEW.root_key IS NOT NULL AND NOT EXISTS(SELECT 1 FROM pa_archive_reference_work WHERE root_key=NEW.root_key);
END;
CREATE TRIGGER pa_reference_work_publications_delete AFTER DELETE ON pa_publications BEGIN
 DELETE FROM pa_archive_reference_work WHERE root_key=OLD.root_key AND NOT EXISTS(SELECT 1 FROM pa_registered_archive_roots r WHERE r.root_key=OLD.root_key);
END;
CREATE TRIGGER pa_reference_work_restores_insert AFTER INSERT ON pa_restores BEGIN
 INSERT INTO pa_archive_reference_work(root_key) SELECT NEW.root_key WHERE NEW.root_key IS NOT NULL AND NOT EXISTS(SELECT 1 FROM pa_archive_reference_work WHERE root_key=NEW.root_key);
END;
CREATE TRIGGER pa_reference_work_restores_update AFTER UPDATE OF root_key ON pa_restores BEGIN
 DELETE FROM pa_archive_reference_work WHERE root_key=OLD.root_key AND NOT EXISTS(SELECT 1 FROM pa_registered_archive_roots r WHERE r.root_key=OLD.root_key);
 INSERT INTO pa_archive_reference_work(root_key) SELECT NEW.root_key WHERE NEW.root_key IS NOT NULL AND NOT EXISTS(SELECT 1 FROM pa_archive_reference_work WHERE root_key=NEW.root_key);
END;
CREATE TRIGGER pa_reference_work_restores_delete AFTER DELETE ON pa_restores BEGIN
 DELETE FROM pa_archive_reference_work WHERE root_key=OLD.root_key AND NOT EXISTS(SELECT 1 FROM pa_registered_archive_roots r WHERE r.root_key=OLD.root_key);
END;
CREATE TRIGGER pa_reference_work_archive_lineage_insert AFTER INSERT ON pa_archive_lineage BEGIN
 INSERT INTO pa_archive_reference_work(root_key) SELECT NEW.source_root WHERE NEW.source_root IS NOT NULL AND NOT EXISTS(SELECT 1 FROM pa_archive_reference_work WHERE root_key=NEW.source_root);
END;
CREATE TRIGGER pa_reference_work_format_work_insert AFTER INSERT ON pa_format_work BEGIN
 INSERT INTO pa_archive_reference_work(root_key) SELECT NEW.old_root WHERE NEW.old_root IS NOT NULL AND NOT EXISTS(SELECT 1 FROM pa_archive_reference_work WHERE root_key=NEW.old_root);
 INSERT INTO pa_archive_reference_work(root_key) SELECT NEW.new_root WHERE NEW.new_root IS NOT NULL AND NOT EXISTS(SELECT 1 FROM pa_archive_reference_work WHERE root_key=NEW.new_root);
END;
CREATE TRIGGER pa_reference_work_format_work_update AFTER UPDATE OF old_root,new_root ON pa_format_work BEGIN
 DELETE FROM pa_archive_reference_work WHERE root_key=OLD.old_root AND NOT EXISTS(SELECT 1 FROM pa_registered_archive_roots r WHERE r.root_key=OLD.old_root);
 DELETE FROM pa_archive_reference_work WHERE root_key=OLD.new_root AND NOT EXISTS(SELECT 1 FROM pa_registered_archive_roots r WHERE r.root_key=OLD.new_root);
 INSERT INTO pa_archive_reference_work(root_key) SELECT NEW.old_root WHERE NEW.old_root IS NOT NULL AND NOT EXISTS(SELECT 1 FROM pa_archive_reference_work WHERE root_key=NEW.old_root);
 INSERT INTO pa_archive_reference_work(root_key) SELECT NEW.new_root WHERE NEW.new_root IS NOT NULL AND NOT EXISTS(SELECT 1 FROM pa_archive_reference_work WHERE root_key=NEW.new_root);
END;
CREATE TRIGGER pa_reference_work_format_work_delete AFTER DELETE ON pa_format_work BEGIN
 DELETE FROM pa_archive_reference_work WHERE root_key=OLD.old_root AND NOT EXISTS(SELECT 1 FROM pa_registered_archive_roots r WHERE r.root_key=OLD.old_root);
 DELETE FROM pa_archive_reference_work WHERE root_key=OLD.new_root AND NOT EXISTS(SELECT 1 FROM pa_registered_archive_roots r WHERE r.root_key=OLD.new_root);
END;
CREATE TRIGGER pa_reference_work_referenced BEFORE DELETE ON pa_archive_reference_work
 WHEN EXISTS(SELECT 1 FROM pa_registered_archive_roots WHERE root_key=OLD.root_key)
 BEGIN SELECT RAISE(ABORT,'pa_reference_still_registered'); END;
CREATE TRIGGER pa_reference_sequence_monotonic BEFORE UPDATE OF reference_sequence ON pa_meta
 WHEN NEW.reference_sequence<OLD.reference_sequence
 BEGIN SELECT RAISE(ABORT,'pa_reference_sequence_aba'); END;
-- Safe populated upgrade: old cursor formats restart; current references get
-- exact monotonic identities. Obsolete registry rows disappear with last reference.
INSERT INTO pa_archive_reference_work(root_key) SELECT root_key FROM pa_registered_archive_roots ORDER BY root_key;
UPDATE pa_publications SET retirement_proof=NULL,retirement_proof_generation=retirement_proof_generation+1 WHERE retirement_proof IS NOT NULL;
UPDATE pa_meta SET schema_version=11 WHERE singleton=1;
