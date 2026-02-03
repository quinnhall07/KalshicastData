# reset_db.py
from __future__ import annotations

from db import get_conn


def reset_db() -> None:
    """Truncate project tables that exist in the current Supabase schema.

    Safe against missing tables.
    """
    candidates = [
        # Metrics
        "public.dashboard_stats",
        "public.forecast_errors",
        # Forecast data
        "public.forecast_extras_hourly",
        "public.forecasts_daily",
        "public.forecast_runs",
        # Observation data
        "public.observations",
        "public.observation_runs",
        # Reference data
        "public.locations",
    ]

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "select relname from pg_catalog.pg_class c "
                "join pg_catalog.pg_namespace n on n.oid = c.relnamespace "
                "where n.nspname='public' and c.relkind='r';"
            )
            existing = {f"public.{r[0]}" for r in cur.fetchall()}

            to_truncate = [t for t in candidates if t in existing]
            if not to_truncate:
                print("No known tables found to truncate.")
                return

            cur.execute(f"TRUNCATE TABLE {', '.join(to_truncate)} RESTART IDENTITY CASCADE;")
        conn.commit()

    print(f"Postgres reset complete. Truncated: {', '.join(to_truncate)}")


if __name__ == "__main__":
    reset_db()
