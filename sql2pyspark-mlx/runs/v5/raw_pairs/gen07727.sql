WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, status_bits + humidity AS status_bits_plus_humidity
    FROM device_readings
    WHERE reading_ts >= TIMESTAMP '2025-09-01 06:00:00'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        reading_ts,
        temp_c,
        pressure,
        status_bits,
        firmware,
        status_bits_plus_humidity,
        LAG(status_bits_plus_humidity) OVER w_running AS prev_status_bits_plus_humidity,
        LEAD(status_bits_plus_humidity) OVER w_running AS next_status_bits_plus_humidity,
        LEAD(temp_c) OVER w_running AS next_temp_c,
        LAG(temp_c) OVER w_running AS prev_temp_c,
        ROW_NUMBER() OVER w_running AS row_seq
    FROM cleaned
    WINDOW w_running AS (PARTITION BY device_code ORDER BY pressure, id)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN temp_c >= 36.1 THEN 'High' WHEN temp_c >= 21.4 THEN 'Medium' ELSE 'Low' END AS temp_c_tier
    FROM metrics
    WHERE temp_c < 37
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(status_bits) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_bits_avg_30m,
        NTILE(4) OVER (ORDER BY prev_status_bits_plus_humidity, id) AS prev_status_bits_plus_humidity_bucket,
        ROW_NUMBER() OVER (ORDER BY status_bits DESC, id) AS event_seq,
        AVG(pressure) OVER (ORDER BY status_bits_plus_humidity, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS pressure_avg_5_prior,
        LAST_VALUE(prev_temp_c) IGNORE NULLS OVER (ORDER BY prev_temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_temp_c
    FROM filtered
),
enriched_device_readings AS (
    -- Ranking and gap features
    SELECT
        id,
        temp_c,
        status_bits_plus_humidity,
        prev_status_bits_plus_humidity,
        next_temp_c,
        row_seq,
        temp_c_tier,
        prev_status_bits_plus_humidity_bucket,
        event_seq,
        pressure_avg_5_prior,
        last_known_prev_temp_c,
        LAG(row_seq) OVER w_part AS prev_row_seq,
        LEAD(row_seq) OVER w_part AS next_row_seq,
        LAG(status_bits_avg_30m) OVER w_part AS prev_status_bits_avg_30m,
        ROW_NUMBER() OVER w_part AS row_seq_2
    FROM enriched
    WINDOW w_part AS (ORDER BY reading_ts, id)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(next_row_seq, 1) OVER (ORDER BY prev_status_bits_plus_humidity_bucket, id) AS next_next_row_seq,
        LAG(row_seq_2, 1) OVER (ORDER BY row_seq_2, id) AS prev_row_seq_2,
        AVG(prev_status_bits_plus_humidity_bucket) OVER (ORDER BY prev_row_seq, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS prev_status_bits_plus_humidity_bucket_avg_6
    FROM enriched_device_readings
    WHERE row_seq_2 > 4
),
enriched_device_readings_2 AS (
    -- Ranking and gap features
    SELECT
        id,
        temp_c,
        status_bits_plus_humidity,
        prev_status_bits_plus_humidity,
        row_seq,
        temp_c_tier,
        prev_status_bits_avg_30m,
        next_next_row_seq,
        prev_row_seq_2,
        prev_status_bits_plus_humidity_bucket_avg_6,
        LAG(prev_status_bits_plus_humidity_bucket_avg_6, 2) OVER (ORDER BY last_known_prev_temp_c, id) AS prev_prev_status_bits_plus_humidity_bucket_avg_6,
        ROW_NUMBER() OVER (ORDER BY temp_c, id) AS event_seq_2,
        ROW_NUMBER() OVER (ORDER BY prev_status_bits_plus_humidity, id) AS seq_no,
        RANK() OVER (ORDER BY next_temp_c) AS next_temp_c_rank
    FROM with_windows
)
-- Multi-level aggregation
SELECT
    seq_no,
    CASE WHEN prev_prev_status_bits_plus_humidity_bucket_avg_6 >= 8 THEN 'LARGE' ELSE 'SMALL' END AS prev_prev_status_bits_plus_humidity_bucket_avg_6_tier,
    COUNT(*) AS row_count,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(seq_no, ARRAY(0.25, 0.5, 0.75)) AS seq_no_quartiles,
    MAX(seq_no) AS max_seq_no,
    MIN(temp_c) AS min_temp_c,
    APPROX_PERCENTILE(prev_prev_status_bits_plus_humidity_bucket_avg_6, 0.5) AS prev_prev_status_bits_plus_humidity_bucket_avg_6_p50
FROM enriched_device_readings_2
GROUP BY seq_no, CASE WHEN prev_prev_status_bits_plus_humidity_bucket_avg_6 >= 8 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY prev_prev_status_bits_plus_humidity_bucket_avg_6_p50 DESC
