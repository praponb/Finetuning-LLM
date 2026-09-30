WITH valid AS (
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        ROUND(pressure, 1) AS pressure_rounded
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
      AND pressure <= 1023
),
filtered AS (
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        pressure_rounded,
        humidity - pressure_rounded AS humidity_minus_pressure_rounded
    FROM valid
    WHERE TO_DATE(reading_ts) < '2024-12-01'
),
base AS (
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        pressure_rounded,
        DATE_TRUNC('week', reading_ts) AS reading_ts_period
    FROM filtered
    WHERE reading_ts >= TIMESTAMP '2025-09-01 05:00:00'
),
with_windows AS (
    SELECT
        id,
        reading_ts,
        temp_c,
        pressure,
        reading_ts_period,
        PERCENT_RANK() OVER (ORDER BY temp_c) AS temp_c_percent_rank,
        LEAD(humidity, 2) OVER (ORDER BY reading_ts, id) AS next_humidity,
        FIRST_VALUE(temp_c) IGNORE NULLS OVER w_recent AS first_known_temp_c,
        PERCENT_RANK() OVER (ORDER BY pressure NULLS LAST) AS pressure_percent_rank,
        FIRST_VALUE(humidity) IGNORE NULLS OVER w_recent AS first_known_humidity
    FROM base
    WINDOW w_recent AS (ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    SELECT
        id,
        reading_ts,
        temp_c,
        reading_ts_period,
        first_known_temp_c,
        first_known_humidity,
        MAX(pressure_percent_rank) OVER (ORDER BY CAST(reading_ts_period AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS pressure_percent_rank_max_7d,
        LAST_VALUE(temp_c_percent_rank) IGNORE NULLS OVER (ORDER BY reading_ts_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_temp_c_percent_rank
    FROM with_windows
)
SELECT
    CASE WHEN temp_c >= 38.5 THEN 'HIGH' ELSE 'LOW' END AS temp_c_tier,
    (UNIX_TIMESTAMP(MAX(reading_ts_period)) - UNIX_TIMESTAMP(MIN(reading_ts_period))) / 60 AS span_minutes,
    MIN(reading_ts) AS started_at,
    MIN(reading_ts_period) AS started_at_2,
    MAX(reading_ts) AS last_reading_ts,
    ROUND(AVG(temp_c), 2) AS avg_temp_c,
    COUNT(DISTINCT id) AS distinct_ids
FROM scored
GROUP BY CASE WHEN temp_c >= 38.5 THEN 'HIGH' ELSE 'LOW' END
