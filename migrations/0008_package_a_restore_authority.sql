-- Scoped PA-FINAL-03 successor. Historical 0001--0007 remain immutable.
-- Persist the target catalogue boundary across restore crashes/restarts.
ALTER TABLE pa_restores ADD COLUMN target_root TEXT;
ALTER TABLE pa_restores ADD COLUMN target_catalogue_generation INTEGER NOT NULL DEFAULT 0 CHECK(target_catalogue_generation>=0);
ALTER TABLE pa_restores ADD COLUMN semantic_index_version INTEGER NOT NULL DEFAULT 0 CHECK(semantic_index_version IN(0,1));
UPDATE pa_restores SET target_root=(SELECT root_key FROM pa_meta),target_catalogue_generation=(SELECT catalogue_generation FROM pa_meta);
UPDATE pa_meta SET schema_version=8 WHERE singleton=1;
