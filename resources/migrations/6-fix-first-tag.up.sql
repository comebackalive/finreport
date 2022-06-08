alter table report drop column first_tag;
alter table report add column first_tag text generated always as (tags[1]) stored;
create index report_first_tag on report (first_tag);
