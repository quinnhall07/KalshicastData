# reset_db.py
from __future__ import annotations

from db import get_conn


def reset_db() -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                TRUNCATE TABLE
                    -- Forecast side
                    forecast_extras_hourly,
                    forecasts_daily,
                    forecast_revisions,
                    forecast_errors,
                    error_stats,
                    forecasts,
                    forecast_runs,

                    -- Observation side
                    observations_v2,
                    observation_runs,
                    observations,

                    -- Core
                    locations
                RESTART IDENTITY CASCADE;
            """)
        conn.commit()

    print("Postgres reset complete.")


if __name__ == "__main__":
    reset_db()
