WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM device_readings
    QUALIFY RANK() OVER (PARTITION BY device_code ORDER BY pressure NULLS LAST) = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(status_bits) OVER w_part AS last4_sum_status_bits,
        MIN(status_bits) OVER w_part AS last4_min_status_bits,
        AVG(status_bits) OVER w_part AS last4_avg_status_bits,
        NTILE(5) OVER (PARTITION BY device_code ORDER BY humidity DESC, id) AS humidity_bucket
    FROM ranked
    WINDOW w_part AS (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM enriched
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
flagged_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 1 HOUR THEN 1 ELSE 0 END AS is_new_session
    FROM sessionized
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        last4_sum_status_bits,
        is_new_burst,
        SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged_2
)
-- Multi-level aggregation
SELECT
    device_code,
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    COUNT_IF(device_code = 'DV-03') AS dv_03_events,
    COUNT(*) AS event_count,
    STDDEV_SAMP(humidity) AS humidity_stddev,
    BIT_OR(status_bits) AS status_bits_bit_or
FROM islands
GROUP BY device_code, DATE_TRUNC('HOUR', reading_ts);
