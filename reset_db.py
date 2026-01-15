from __future__ import annotations

from db import get_conn


def reset_db() -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                TRUNCATE TABLE
                    forecast_revisions,
                    forecast_errors,
                    error_stats,
                    forecasts,
                    forecast_runs,
                    observations,
                    locations
                RESTART IDENTITY CASCADE;
            """)
        conn.commit()

    print("Postgres reset complete.")


if __name__ == "__main__":
    reset_db()
