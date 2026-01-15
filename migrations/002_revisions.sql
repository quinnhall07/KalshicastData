-- migrations/002_revisions.sql
create table if not exists public.forecast_revisions (
  station_id text not null references public.locations(station_id) on delete cascade,
  source text not null,
  kind text not null check (kind in ('high','low')),
  target_date date not null,

  issued_at timestamptz not null,
  forecast_f double precision,

  prev_issued_at timestamptz,
  prev_forecast_f double precision,
  delta_f double precision,

  created_at timestamptz not null default now(),

  primary key (station_id, source, kind, target_date, issued_at)
);

create index if not exists idx_revisions_lookup
  on public.forecast_revisions(station_id, target_date, source, kind);

create index if not exists idx_revisions_source_date
  on public.forecast_revisions(source, target_date, kind);
