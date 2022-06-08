alter table report add column first_tag text generated always as (tags[0]) stored;
drop index report_tag;
create index report_first_tag on report (first_tag);
