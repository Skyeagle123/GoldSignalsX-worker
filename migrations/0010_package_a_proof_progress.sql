-- Bounded progress lives on its publication, not one row per archive edge.
ALTER TABLE pa_publications ADD COLUMN retirement_proof TEXT CHECK(retirement_proof IS NULL OR (json_valid(retirement_proof) AND length(CAST(retirement_proof AS BLOB))<=8192));
ALTER TABLE pa_publications ADD COLUMN retirement_proof_generation INTEGER NOT NULL DEFAULT 0 CHECK(retirement_proof_generation BETWEEN 0 AND 9007199254740991);
ALTER TABLE pa_meta ADD COLUMN archive_reference_revision INTEGER NOT NULL DEFAULT 0 CHECK(archive_reference_revision BETWEEN 0 AND 9007199254740991);

ALTER TABLE pa_meta ADD COLUMN retirement_cursor TEXT NOT NULL DEFAULT '';

-- Every legitimate addition/removal/replacement of a registered root advances
-- this durable boundary, including ABA. Unrelated record/audit writes do not.
CREATE TRIGGER pa_reference_meta AFTER UPDATE OF root_key,catalogue_generation ON pa_meta
 WHEN NEW.root_key IS NOT OLD.root_key OR NEW.catalogue_generation!=OLD.catalogue_generation
 BEGIN UPDATE pa_meta SET archive_reference_revision=archive_reference_revision+1; END;
CREATE TRIGGER pa_reference_scope_insert AFTER INSERT ON pa_scopes WHEN NEW.archive_root IS NOT NULL BEGIN UPDATE pa_meta SET archive_reference_revision=archive_reference_revision+1; END;
CREATE TRIGGER pa_reference_scope_update AFTER UPDATE OF archive_root ON pa_scopes WHEN NEW.archive_root IS NOT OLD.archive_root BEGIN UPDATE pa_meta SET archive_reference_revision=archive_reference_revision+1; END;
CREATE TRIGGER pa_reference_scope_delete AFTER DELETE ON pa_scopes WHEN OLD.archive_root IS NOT NULL BEGIN UPDATE pa_meta SET archive_reference_revision=archive_reference_revision+1; END;
CREATE TRIGGER pa_reference_publication_insert AFTER INSERT ON pa_publications WHEN NEW.root_key IS NOT NULL BEGIN UPDATE pa_meta SET archive_reference_revision=archive_reference_revision+1; END;
CREATE TRIGGER pa_reference_publication_update AFTER UPDATE OF root_key ON pa_publications WHEN NEW.root_key IS NOT OLD.root_key BEGIN UPDATE pa_meta SET archive_reference_revision=archive_reference_revision+1; END;
CREATE TRIGGER pa_reference_publication_delete AFTER DELETE ON pa_publications WHEN OLD.root_key IS NOT NULL BEGIN UPDATE pa_meta SET archive_reference_revision=archive_reference_revision+1; END;
CREATE TRIGGER pa_reference_restore_insert AFTER INSERT ON pa_restores BEGIN UPDATE pa_meta SET archive_reference_revision=archive_reference_revision+1; END;
CREATE TRIGGER pa_reference_restore_update AFTER UPDATE OF root_key ON pa_restores WHEN NEW.root_key IS NOT OLD.root_key BEGIN UPDATE pa_meta SET archive_reference_revision=archive_reference_revision+1; END;
CREATE TRIGGER pa_reference_restore_delete AFTER DELETE ON pa_restores BEGIN UPDATE pa_meta SET archive_reference_revision=archive_reference_revision+1; END;
CREATE TRIGGER pa_reference_lineage_insert AFTER INSERT ON pa_archive_lineage BEGIN UPDATE pa_meta SET archive_reference_revision=archive_reference_revision+1; END;
CREATE TRIGGER pa_reference_format_insert AFTER INSERT ON pa_format_work BEGIN UPDATE pa_meta SET archive_reference_revision=archive_reference_revision+1; END;
CREATE TRIGGER pa_reference_format_update AFTER UPDATE OF old_root,new_root ON pa_format_work WHEN NEW.old_root IS NOT OLD.old_root OR NEW.new_root IS NOT OLD.new_root BEGIN UPDATE pa_meta SET archive_reference_revision=archive_reference_revision+1; END;
CREATE TRIGGER pa_reference_format_delete AFTER DELETE ON pa_format_work BEGIN UPDATE pa_meta SET archive_reference_revision=archive_reference_revision+1; END;
UPDATE pa_meta SET schema_version=10 WHERE singleton=1;
