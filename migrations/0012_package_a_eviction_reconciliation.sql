-- One replaceable cursor/receipt per already bounded publication (maximum 64).
ALTER TABLE pa_publications ADD COLUMN eviction_progress TEXT CHECK(eviction_progress IS NULL OR (json_valid(eviction_progress) AND length(CAST(eviction_progress AS BLOB))<=8192));
ALTER TABLE pa_publications ADD COLUMN eviction_generation INTEGER NOT NULL DEFAULT 0 CHECK(eviction_generation BETWEEN 0 AND 9007199254740991);
ALTER TABLE pa_publications ADD COLUMN eviction_owner TEXT CHECK(eviction_owner IS NULL OR length(CAST(eviction_owner AS BLOB)) BETWEEN 1 AND 1024);
ALTER TABLE pa_publications ADD COLUMN eviction_nonce TEXT;
ALTER TABLE pa_publications ADD COLUMN eviction_until INTEGER NOT NULL DEFAULT 0 CHECK(eviction_until>=0);
ALTER TABLE pa_publications ADD COLUMN eviction_charged INTEGER NOT NULL DEFAULT 0 CHECK(eviction_charged IN(0,1));
-- 16x the maximum cursor plus four row/index/temp allowances. No deletion credit.
CREATE TRIGGER pa_eviction_progress_capacity BEFORE UPDATE OF eviction_progress ON pa_publications
 WHEN NEW.eviction_progress IS NOT NULL AND OLD.eviction_charged=0 AND (
 (SELECT physical_charged FROM pa_meta)+(SELECT COALESCE(SUM(bytes),0) FROM pa_reservations WHERE state='RESERVED')+196608>5500000000 OR
 (SELECT charged FROM pa_pool_use WHERE pool='archive')+(SELECT COALESCE(SUM(bytes),0) FROM pa_reservations WHERE pool='archive' AND state='RESERVED')+196608>400000000)
 BEGIN SELECT RAISE(ABORT,'pa_eviction_progress_capacity'); END;
CREATE TRIGGER pa_eviction_progress_charge AFTER UPDATE OF eviction_progress ON pa_publications
 WHEN NEW.eviction_progress IS NOT NULL AND OLD.eviction_charged=0
 BEGIN
 UPDATE pa_meta SET physical_charged=physical_charged+196608;
 UPDATE pa_pool_use SET charged=charged+196608 WHERE pool='archive';
 UPDATE pa_publications SET eviction_charged=1 WHERE id=NEW.id;
 END;
CREATE TRIGGER pa_eviction_progress_monotonic BEFORE UPDATE OF eviction_generation,eviction_charged ON pa_publications
 WHEN NEW.eviction_generation<OLD.eviction_generation OR NEW.eviction_charged<OLD.eviction_charged
 BEGIN SELECT RAISE(ABORT,'pa_eviction_progress_aba'); END;
-- Replacing an epoch and then restoring its text must not revive a cursor.
CREATE TRIGGER pa_eviction_epoch_boundary AFTER UPDATE OF epoch ON pa_meta
 WHEN NEW.epoch IS NOT OLD.epoch
 BEGIN UPDATE pa_meta SET archive_reference_revision=archive_reference_revision+1; END;
UPDATE pa_meta SET schema_version=12 WHERE singleton=1;
