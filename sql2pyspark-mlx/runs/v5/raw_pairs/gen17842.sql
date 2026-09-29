WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(humidity / temp_c, 2) AS humidity_per_temp_c
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(humidity_per_temp_c, 1) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_humidity_per_temp_c,
        CORR(pressure, humidity) OVER (PARTITION BY device_code) AS corr_pressure_humidity,
        AVG(humidity_per_temp_c) OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS humidity_per_temp_c_avg_3,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure DESC, id) AS seq_no,
        CUME_DIST() OVER (PARTITION BY device_code ORDER BY temp_c DESC NULLS LAST) AS temp_c_cume_dist
    FROM filtered
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM scored
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        firmware,
        prev_humidity_per_temp_c,
        corr_pressure_humidity,
        SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        device_code,
        reading_ts,
        prev_humidity_per_temp_c,
        island_id,
        CUME_DIST() OVER (PARTITION BY device_code ORDER BY island_id DESC) AS island_id_cume_dist,
        NTILE(5) OVER (ORDER BY corr_pressure_humidity DESC, id) AS corr_pressure_humidity_bucket,
        RANK() OVER (PARTITION BY device_code ORDER BY prev_humidity_per_temp_c DESC NULLS LAST) AS prev_humidity_per_temp_c_rank
    FROM islands
),
kept_keys AS (
    -- Set difference on keys
    SELECT device_code, id
    FROM with_windows
    EXCEPT DISTINCT
    SELECT device_code, id
    FROM device_readings
    WHERE firmware = '1.1.0'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.device_code = k.device_code AND s.id = k.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        device_code,
        reading_ts,
        prev_humidity_per_temp_c,
        island_id_cume_dist,
        corr_pressure_humidity_bucket,
        prev_humidity_per_temp_c_rank,
        ROUND(island_id_cume_dist * 0.9, 2) AS adjusted_island_id_cume_dist
    FROM verified
    WHERE island_id < 2
),
metrics AS (
    -- Ranking and gap features
    SELECT
        device_code,
        prev_humidity_per_temp_c,
        island_id_cume_dist,
        corr_pressure_humidity_bucket,
        RANK() OVER (PARTITION BY device_code ORDER BY prev_humidity_per_temp_c_rank NULLS FIRST) AS prev_humidity_per_temp_c_rank_rank
    FROM valid
)
-- Aggregate with conditional metrics
SELECT
    device_code,
    PERCENTILE_APPROX(prev_humidity_per_temp_c_rank_rank, ARRAY(0.25, 0.5, 0.75)) AS prev_humidity_per_temp_c_rank_rank_quartiles,
    COUNT(CASE WHEN device_code = 'DV-01' THEN 1 END) AS dv_01_count,
    COUNT(DISTINCT device_code) AS distinct_device_codes,
    PERCENTILE_APPROX(corr_pressure_humidity_bucket, ARRAY(0.25, 0.5, 0.75)) AS corr_pressure_humidity_bucket_quartiles,
    APPROX_PERCENTILE(prev_humidity_per_temp_c_rank_rank, ARRAY(0.25, 0.5, 0.75)) AS prev_humidity_per_temp_c_rank_rank_quartiles_2,
    COUNT(*) AS num_rows
FROM metrics
GROUP BY device_code;
