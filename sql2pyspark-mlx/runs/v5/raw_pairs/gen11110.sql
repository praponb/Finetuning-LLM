WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(reading_ts AS DATE) AS reading_ts_day
    FROM device_readings
    WHERE temp_c <= 34
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device_code,
        pressure,
        status_bits,
        reading_ts_day,
        AVG(status_bits) OVER (PARTITION BY device_code ORDER BY reading_ts_day, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS status_bits_avg_5_prior,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c DESC, id) AS seq_no,
        RANK() OVER (PARTITION BY device_code ORDER BY humidity DESC NULLS FIRST) AS humidity_rank,
        ROUND(temp_c / SUM(temp_c) OVER (PARTITION BY device_code), 4) AS temp_c_share,
        NTILE(4) OVER (ORDER BY status_bits DESC, id) AS status_bits_bucket
    FROM base
    WHERE device_code = 'DV-01'
),
scored_device_readings AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device_code,
        pressure,
        status_bits,
        seq_no,
        status_bits_bucket,
        AVG(status_bits_avg_5_prior) OVER w_running AS device_code_avg_status_bits_avg_5_prior,
        LAST_VALUE(pressure) IGNORE NULLS OVER w_ordered AS last_known_pressure,
        RANK() OVER (ORDER BY status_bits_avg_5_prior DESC) AS status_bits_avg_5_prior_rank
    FROM scored
    WINDOW w_running AS (PARTITION BY device_code), w_ordered AS (ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits_bucket NULLS FIRST, id) AS dedup_rank
    FROM scored_device_readings
    QUALIFY dedup_rank = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        status_bits_bucket,
        LAG(last_known_pressure) OVER (PARTITION BY device_code ORDER BY device_code_avg_status_bits_avg_5_prior, id) AS prev_last_known_pressure,
        LEAD(seq_no) OVER (PARTITION BY device_code ORDER BY device_code_avg_status_bits_avg_5_prior, id) AS next_seq_no,
        LAG(seq_no) OVER (PARTITION BY device_code ORDER BY device_code_avg_status_bits_avg_5_prior, id) AS prev_seq_no,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY device_code_avg_status_bits_avg_5_prior, id) AS event_seq,
        AVG(seq_no) OVER (ORDER BY status_bits_bucket, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS seq_no_avg_3
    FROM latest
)
-- Multi-level aggregation
SELECT
    CASE WHEN next_seq_no >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_seq_no_tier,
    APPROX_PERCENTILE(seq_no_avg_3, 0.5) AS seq_no_avg_3_p50,
    ROUND(SUM(prev_last_known_pressure) / NULLIF(COUNT(CASE WHEN device_code = 'DV-03' THEN 1 END), 0), 2) AS prev_last_known_pressure_per_dv_03,
    COUNT(*) AS num_rows
FROM with_windows
GROUP BY 1
ORDER BY prev_last_known_pressure_per_dv_03 DESC NULLS LAST
