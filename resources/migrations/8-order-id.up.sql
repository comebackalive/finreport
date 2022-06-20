alter table report add column order_id text;
alter table report add unique (order_id);
