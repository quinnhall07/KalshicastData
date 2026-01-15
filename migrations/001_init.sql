-- migrations/001_init.sql
create extension if not exists pgcrypto;

create table if not exists public.locations (
  station_id text primary key,
  name text,
  state text,
  timezone text,
  lat double precision,
  lon double precision,
  elevation_ft double precision,
  is_active boolean not null default true
);

create table if not exists public.forecast_runs (
  run_id uuid primary key default gen_random_uuid(),
  source text not null,
  issued_at timestamptz not null,
  fetched_at timestamptz not null default now(),
  meta jsonb not null default '{}'::jsonb,
  unique (source, issued_at)
);

create table if not exists public.forecasts (
  run_id uuid not null references public.forecast_runs(run_id) on delete cascade,
  station_id text not null references public.locations(station_id) on delete cascade,
  target_date date not null,
  kind text not null check (kind in ('high','low')),
  value_f double precision,
  lead_hours double precision,

  dewpoint_f double precision,
  humidity_pct double precision,
  wind_speed_mph double precision,
  wind_dir_deg double precision,
  cloud_cover_pct double precision,
  precip_prob_pct double precision,

  created_at timestamptz not null default now(),
  primary key (run_id, station_id, target_date, kind)
);

create table if not exists public.observations (
  station_id text not null references public.locations(station_id) on delete cascade,
  date date not null,
  observed_high double precision,
  observed_low double precision,
  issued_at timestamptz,
  fetched_at timestamptz not null default now(),
  raw_text text,
  source text not null default 'nws_station_obs',
  primary key (station_id, date)
);

create table if not exists public.forecast_errors (
  forecast_id text primary key, -- run_id|station|date|kind
  station_id text not null references public.locations(station_id) on delete cascade,
  source text not null,
  target_date date not null,
  kind text not null check (kind in ('high','low')),
  issued_at timestamptz not null,
  lead_hours double precision,
  forecast_f double precision,
  observed_f double precision,
  error_f double precision,
  abs_error_f double precision,
  created_at timestamptz not null default now()
);

create table if not exists public.error_stats (
  station_id text references public.locations(station_id) on delete cascade,
  source text not null,
  kind text not null check (kind in ('high','low','both')),
  window_days int not null,
  n int not null,
  bias double precision,
  mae double precision,
  rmse double precision,
  p10 double precision,
  p50 double precision,
  p90 double precision,
  last_updated timestamptz not null default now(),
  primary key (station_id, source, kind, window_days)
);

create index if not exists idx_forecasts_station_date on public.forecasts(station_id, target_date);
create index if not exists idx_observations_station_date on public.observations(station_id, date);
create index if not exists idx_errors_station_date on public.forecast_errors(station_id, target_date);
create index if not exists idx_errors_source_kind on public.forecast_errors(source, kind);
