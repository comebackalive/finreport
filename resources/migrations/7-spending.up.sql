create table spending (
  id serial primary key,
  fname text not null, -- file name data is coming from
  bank text not null,
  date timestamptz not null,
  created_at timestamptz not null default now(),
  amount numeric(12,2),
  "name" text,
  target text
);
