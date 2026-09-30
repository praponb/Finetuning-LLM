WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM device_readings
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(3) OVER (ORDER BY status_bits ASC, id ASC) AS status_bits_bucket,
        COUNT(is_new_burst) OVER (ORDER BY CAST(reading_ts AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS is_new_burst_count_2h,
        NTILE(4) OVER w_grp AS pressure_bucket,
        SUM(session_id) OVER w_hist AS running_session_id,
        PERCENT_RANK() OVER (PARTITION BY device_code ORDER BY session_id DESC) AS session_id_percent_rank
    FROM sessions_device_readings
    WINDOW w_grp AS (PARTITION BY device_code ORDER BY pressure ASC, id ASC), w_hist AS (ORDER BY is_new_burst ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) + INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS is_new_burst_2
    FROM scored
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst_2) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps_2
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        is_new_burst,
        pressure_bucket,
        running_session_id,
        session_id_percent_rank,
        is_new_burst_2,
        burst_id,
        status_bits_bucket + is_new_burst_2 AS status_bits_bucket_plus_is_new_burst_2,
        ROUND(status_bits * 1.07, 2) AS status_bits_adjusted
    FROM islands
    WHERE reading_ts >= TIMESTAMP '2025-09-01 12:00:00'
      AND session_id > 4
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        firmware,
        is_new_burst,
        pressure_bucket,
        running_session_id,
        is_new_burst_2,
        burst_id,
        status_bits_bucket_plus_is_new_burst_2,
        status_bits_adjusted,
        CASE WHEN running_session_id >= 36 THEN 'High' WHEN running_session_id >= 9 THEN 'Medium' ELSE 'Low' END AS running_session_id_tier,
        status_bits - humidity AS status_bits_minus_humidity
    FROM base
    WHERE reading_ts >= DATE '2025-09-01'
      AND is_new_burst_2 <= 0
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN burst_id >= 8 THEN 'LARGE' ELSE 'SMALL' END AS burst_id_tier,
    running_session_id,
    burst_id,
    COUNT(*) AS row_count,
    COUNT(DISTINCT id) AS distinct_ids,
    MIN(reading_ts) AS first_seen,
    MIN(reading_ts) AS started_at
FROM cleaned
GROUP BY CASE WHEN burst_id >= 8 THEN 'LARGE' ELSE 'SMALL' END, running_session_id, burst_id
ORDER BY burst_id_tier DESC
