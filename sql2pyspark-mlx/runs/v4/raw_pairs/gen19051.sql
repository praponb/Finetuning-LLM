WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, temp_c - humidity AS temp_c_minus_humidity
    FROM device_readings
    WHERE reading_ts >= TIMESTAMP '2025-09-01 12:00:00'
      AND humidity > 70
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(temp_c_minus_humidity, 1) OVER (ORDER BY temp_c_minus_humidity, id) AS next_temp_c_minus_humidity,
        FIRST_VALUE(pressure) IGNORE NULLS OVER (ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_pressure,
        ROW_NUMBER() OVER w_recent AS seq_no,
        PERCENT_RANK() OVER (PARTITION BY device_code ORDER BY temp_c DESC NULLS LAST) AS temp_c_percent_rank,
        AVG(temp_c_minus_humidity) OVER w_part AS temp_c_minus_humidity_avg_2_prior
    FROM valid
    WINDOW w_recent AS (PARTITION BY device_code ORDER BY humidity DESC, id), w_part AS (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM enriched
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT firmware
    FROM sessions_device_readings
    INTERSECT
    SELECT firmware
    FROM device_readings
    WHERE status_bits <> 11
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessions_device_readings s
    JOIN remaining_keys k ON s.firmware = k.firmware
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY device_code ORDER BY burst_id DESC) AS burst_id_rank,
        SUM(temp_c_minus_humidity) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS temp_c_minus_humidity_sum_1h
    FROM reconciled
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        temp_c_minus_humidity,
        next_temp_c_minus_humidity,
        first_known_pressure,
        seq_no,
        temp_c_percent_rank,
        temp_c_minus_humidity_avg_2_prior,
        new_group_flag,
        burst_id,
        burst_id_rank,
        temp_c_minus_humidity_sum_1h,
        CUME_DIST() OVER (PARTITION BY device_code ORDER BY burst_id_rank DESC NULLS LAST) AS burst_id_rank_cume_dist,
        AVG(first_known_pressure) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS first_known_pressure_avg_1h,
        CORR(pressure, humidity) OVER (PARTITION BY device_code) AS corr_pressure_humidity
    FROM scored
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(corr_pressure_humidity) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS corr_pressure_humidity_avg_15m,
        CUME_DIST() OVER (PARTITION BY device_code ORDER BY first_known_pressure) AS first_known_pressure_cume_dist
    FROM with_windows
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    CASE WHEN first_known_pressure_cume_dist >= 0.75 THEN 'LARGE' ELSE 'SMALL' END AS first_known_pressure_cume_dist_tier,
    PERCENTILE_APPROX(burst_id, 0.9) AS burst_id_p90,
    MIN(reading_ts) AS started_at,
    ROUND(AVG(burst_id_rank), 2) AS avg_burst_id_rank
FROM metrics
GROUP BY DATE_TRUNC('DAY', reading_ts), CASE WHEN first_known_pressure_cume_dist >= 0.75 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY burst_id_p90 DESC;
