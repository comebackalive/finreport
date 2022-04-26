alter table report add column tags text[];
alter table report add column hiddens text[];

create index report_tag on report ((tags[1]));
create index report_tags on report using gin (tags);
