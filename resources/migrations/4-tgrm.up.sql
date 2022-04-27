create extension if not exists pg_trgm;
CREATE INDEX report_comment_trgm ON report USING gin (comment gin_trgm_ops);
CREATE INDEX report_comment_lower_trgm ON report USING gin (lower(comment) gin_trgm_ops);
