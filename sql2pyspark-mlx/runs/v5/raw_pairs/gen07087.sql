WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c DESC, id) AS row_seq,
        CUME_DIST() OVER (ORDER BY status_bits DESC) AS status_bits_cume_dist,
        RANK() OVER (PARTITION BY device_code ORDER BY status_bits DESC) AS status_bits_rank
    FROM device_readings
    WHERE humidity < 67
),
with_windows_device_readings AS (
    -- Ranking and gap features
    SELECT
        id,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        row_seq,
        status_bits_cume_dist,
        CORR(status_bits, pressure) OVER (PARTITION BY device_code) AS corr_status_bits_pressure,
        NTILE(10) OVER w_running AS status_bits_cume_dist_bucket,
        LEAD(status_bits_cume_dist, 1) OVER w_running AS next_status_bits_cume_dist,
        AVG(humidity) OVER (PARTITION BY device_code ORDER BY status_bits_cume_dist, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS humidity_avg_3_prior
    FROM with_windows
    WINDOW w_running AS (PARTITION BY device_code ORDER BY status_bits_cume_dist, id)
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM with_windows_device_readings
    EXCEPT ALL
    SELECT id
    FROM device_readings
    WHERE humidity > 63
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows_device_readings s
    JOIN remaining_keys k ON s.id = k.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(status_bits_cume_dist_bucket * 1.15, 2) AS status_bits_cume_dist_bucket_scaled,
        CAST(corr_status_bits_pressure AS INT) AS corr_status_bits_pressure_int
    FROM kept_rows
    WHERE status_bits_cume_dist_bucket IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(status_bits) OVER w_part AS status_bits_max_2h,
        AVG(row_seq) OVER w_part AS row_seq_avg_2h
    FROM filtered
    WINDOW w_part AS (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(row_seq_avg_2h, 2) AS row_seq_avg_2h_rounded
    FROM metrics
    WHERE status_bits_cume_dist BETWEEN 0.25 AND 0.27
),
metrics_device_readings AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER w_recent AS status_bits_max_2h_cume_dist,
        CUME_DIST() OVER (ORDER BY status_bits_cume_dist NULLS FIRST) AS status_bits_cume_dist_cume_dist,
        PERCENT_RANK() OVER (ORDER BY corr_status_bits_pressure_int DESC) AS corr_status_bits_pressure_int_percent_rank,
        PERCENT_RANK() OVER (ORDER BY row_seq DESC) AS row_seq_percent_rank,
        RANK() OVER (ORDER BY next_status_bits_cume_dist DESC) AS next_status_bits_cume_dist_rank
    FROM base
    WINDOW w_recent AS (ORDER BY status_bits_max_2h NULLS LAST)
),
grouped AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('HOUR', reading_ts) AS hour_window,
        PERCENTILE_APPROX(row_seq_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS row_seq_percent_rank_quartiles,
        APPROX_PERCENTILE(row_seq_avg_2h, ARRAY(0.25, 0.5, 0.75)) AS row_seq_avg_2h_quartiles,
        COUNT(DISTINCT id) AS distinct_ids
    FROM metrics_device_readings
    GROUP BY DATE_TRUNC('HOUR', reading_ts)
)
SELECT
    hour_window,
    row_seq_percent_rank_quartiles,
    row_seq_avg_2h_quartiles,
    distinct_ids
FROM grouped;
