-- migrations/001_init.sql
-- Lean schema optimized for storage + deletion efficiency (partitioned hourly table)

create extension if not exists pgcrypto;

-- -------------------------
-- locations
-- -------------------------
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

-- -------------------------
-- forecast_runs (no meta)
-- -------------------------
create table if not exists public.forecast_runs (
  run_id uuid primary key default gen_random_uuid(),
  source text not null,
  issued_at timestamptz not null,
  fetched_at timestamptz not null default now(),

  -- IMPORTANT:
  -- This unique constraint is not about storage; it's for correctness/idempotency.
  -- It prevents duplicate runs for the same source+issued_at and enables safe upserts.
  unique (source, issued_at)
);

create index if not exists idx_forecast_runs_source_issued
  on public.forecast_runs(source, issued_at desc);

-- -------------------------
-- forecasts_daily (condensed: high+low in one row)
-- -------------------------
create table if not exists public.forecasts_daily (
  run_id uuid not null references public.forecast_runs(run_id) on delete cascade,
  station_id text not null references public.locations(station_id) on delete cascade,
  target_date date not null,

  high_f real,
  low_f real,

  lead_hours_high real,
  lead_hours_low real,

  created_at timestamptz not null default now(),
  primary key (run_id, station_id, target_date)
);

create index if not exists idx_forecasts_daily_station_date
  on public.forecasts_daily(station_id, target_date);

create index if not exists idx_forecasts_daily_run
  on public.forecasts_daily(run_id);

-- -------------------------
-- forecast_extras_hourly (partitioned by valid_time)
-- No extras json, no created_at
-- -------------------------
create table if not exists public.forecast_extras_hourly (
  run_id uuid not null references public.forecast_runs(run_id) on delete cascade,
  station_id text not null references public.locations(station_id) on delete cascade,
  valid_time timestamptz not null,

  temperature_f real,
  dewpoint_f real,
  humidity_pct real,
  wind_speed_mph real,
  wind_dir_deg smallint,
  cloud_cover_pct real,
  precip_prob_pct real,

  primary key (run_id, station_id, valid_time)
) partition by range (valid_time);

-- Helper function to create monthly partitions (cheap deletion via DROP PARTITION)
create or replace function public.ensure_forecast_extras_hourly_partition(p_month_start date)
returns void
language plpgsql
as $$
declare
  start_ts timestamptz;
  end_ts   timestamptz;
  part_name text;
begin
  start_ts := (p_month_start::timestamptz);
  end_ts   := ((p_month_start + interval '1 month')::timestamptz);
  part_name := format('forecast_extras_hourly_%s', to_char(p_month_start, 'YYYY_MM'));

  execute format(
    'create table if not exists public.%I partition of public.forecast_extras_hourly
     for values from (%L) to (%L)',
    part_name, start_ts, end_ts
  );

  -- Index per partition to support queries by station+time (optional but usually worth it)
  execute format(
    'create index if not exists %I on public.%I (station_id, valid_time)',
    format('idx_%s_station_time', part_name),
    part_name
  );

  execute format(
    'create index if not exists %I on public.%I (run_id)',
    format('idx_%s_run', part_name),
    part_name
  );
end;
$$;

-- Create partitions for a rolling horizon (past 3 months through next 24 months)
do $$
declare
  m date;
  start_m date := date_trunc('month', (now() at time zone 'utc')::date)::date - interval '3 months';
  end_m   date := date_trunc('month', (now() at time zone 'utc')::date)::date + interval '24 months';
begin
  m := start_m;
  while m <= end_m loop
    perform public.ensure_forecast_extras_hourly_partition(m);
    m := (m + interval '1 month')::date;
  end loop;
end $$;

-- Parent-table indexes (planner can use them; partitions still have their own)
create index if not exists idx_forecast_extras_hourly_station_time
  on public.forecast_extras_hourly(station_id, valid_time);

create index if not exists idx_forecast_extras_hourly_run
  on public.forecast_extras_hourly(run_id);

-- -------------------------
-- observation_runs (no changes)
-- -------------------------
create table if not exists public.observation_runs (
  run_id uuid primary key default gen_random_uuid(),
  run_issued_at timestamptz not null,
  fetched_at timestamptz not null default now(),
  unique (run_issued_at)
);

create index if not exists idx_observation_runs_issued
  on public.observation_runs(run_issued_at desc);

-- -------------------------
-- observations (renamed from observations_v2)
-- flagged_raw_text only when suspicious / parse issues
-- -------------------------
create table if not exists public.observations (
  run_id uuid not null references public.observation_runs(run_id) on delete cascade,
  station_id text not null references public.locations(station_id) on delete cascade,
  date date not null,

  observed_high real,
  observed_low real,
  source text not null,

  flagged_raw_text text,
  flagged_reason text,

  primary key (run_id, station_id, date)
);

create index if not exists idx_observations_station_date
  on public.observations(station_id, date);

create index if not exists idx_observations_run
  on public.observations(run_id);

-- -------------------------
-- forecast_errors (thin derived table; join to runs/observations for details)
-- -------------------------
create table if not exists public.forecast_errors (
  forecast_run_id uuid not null references public.forecast_runs(run_id) on delete cascade,
  observation_run_id uuid not null references public.observation_runs(run_id) on delete cascade,
  station_id text not null references public.locations(station_id) on delete cascade,
  target_date date not null,

  ae_high real,
  ae_low real,
  mae real,       -- e.g., (ae_high + ae_low)/2 when both present

  created_at timestamptz not null default now(),
  primary key (forecast_run_id, observation_run_id, station_id, target_date)
);

create index if not exists idx_forecast_errors_station_date
  on public.forecast_errors(station_id, target_date);

create index if not exists idx_forecast_errors_forecast_run
  on public.forecast_errors(forecast_run_id);

create index if not exists idx_forecast_errors_obs_run
  on public.forecast_errors(observation_run_id);

-- -------------------------
-- dashboard_stats (renamed from error_stats; keep all columns)
-- -------------------------
create table if not exists public.dashboard_stats (
  station_id text references public.locations(station_id) on delete cascade,
  source text not null,
  kind text not null check (kind in ('high','low','both')),
  window_days int not null,
  n int not null,

  bias real,
  mae real,
  rmse real,
  p10 real,
  p50 real,
  p90 real,

  last_updated timestamptz not null default now(),
  primary key (station_id, source, kind, window_days)
);

create index if not exists idx_dashboard_stats_station
  on public.dashboard_stats(station_id);

create index if not exists idx_dashboard_stats_source_kind
  on public.dashboard_stats(source, kind);
