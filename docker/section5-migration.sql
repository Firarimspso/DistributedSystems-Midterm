ALTER TABLE object_versions
    ADD COLUMN IF NOT EXISTS tags JSON NOT NULL DEFAULT '{}'::json;
