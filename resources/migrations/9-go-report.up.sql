CREATE TABLE go_report (
    id serial primary key,
    fname text NOT NULL,
    date timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    amount numeric(12,2),
    comment text,
    bank text NOT NULL,
    tags text[],
    hiddens text[],
    first_tag text GENERATED ALWAYS AS (tags[1]) STORED,
    order_id text
);

alter table go_report add unique (order_id);
create index go_report_first_tag on go_report (first_tag);
create index go_report_tags on go_report using gin (tags);
CREATE INDEX go_report_comment_trgm ON go_report USING gin (comment gin_trgm_ops);
CREATE INDEX go_report_comment_lower_trgm ON go_report USING gin (lower(comment) gin_trgm_ops);
create index if not exists go_report_bank on go_report (bank);
create index if not exists go_report_created_at on go_report (created_at);
create index if not exists go_report_date on go_report (date);
