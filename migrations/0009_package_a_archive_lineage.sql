-- Additive residual recovery metadata. Historical 0001--0008 are immutable.
-- A completed, fenced restore authorizes one archive transition per target epoch.
CREATE TABLE pa_archive_lineage(
 target_epoch TEXT PRIMARY KEY,
 source_root TEXT NOT NULL,
 source_epoch TEXT NOT NULL,
 source_generation INTEGER NOT NULL CHECK(source_generation>0),
 restore_id TEXT NOT NULL UNIQUE REFERENCES pa_restores(id) ON DELETE RESTRICT,
 transition_key TEXT NOT NULL UNIQUE,
 transition_wire TEXT NOT NULL CHECK(length(CAST(transition_wire AS BLOB))<=8192),
 CHECK(target_epoch!=source_epoch)
);
CREATE TRIGGER pa_archive_lineage_immutable_update BEFORE UPDATE ON pa_archive_lineage BEGIN SELECT RAISE(ABORT,'pa_lineage_immutable'); END;
CREATE TRIGGER pa_archive_lineage_immutable_delete BEFORE DELETE ON pa_archive_lineage BEGIN SELECT RAISE(ABORT,'pa_lineage_immutable'); END;
UPDATE pa_meta SET schema_version=9 WHERE singleton=1;
