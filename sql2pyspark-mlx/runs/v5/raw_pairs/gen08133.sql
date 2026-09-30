WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(temp_c / SUM(temp_c) OVER (PARTITION BY device_code), 4) AS temp_c_share,
        MIN(pressure) OVER (PARTITION BY device_code) AS device_code_min_pressure,
        AVG(status_bits) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_bits_avg_30m,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c, id) AS seq_no
    FROM device_readings
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM scored
    QUALIFY RANK() OVER (PARTITION BY device_code ORDER BY status_bits DESC) = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(pressure) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS pressure_avg_2_prior,
        SUM(pressure) OVER (PARTITION BY device_code ORDER BY status_bits_avg_30m, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_pressure,
        SUM(seq_no) OVER (PARTITION BY device_code ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_seq_no,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity, id) AS event_seq,
        AVG(status_bits_avg_30m) OVER (PARTITION BY device_code) AS device_code_avg_status_bits_avg_30m
    FROM ranked
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        reading_ts,
        temp_c,
        status_bits,
        temp_c_share,
        device_code_min_pressure,
        cumulative_pressure,
        LAG(pressure, 1) OVER (PARTITION BY device_code ORDER BY running_seq_no, id) AS prev_pressure,
        COUNT(pressure) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS pressure_count_2h
    FROM enriched
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    BIT_OR(status_bits) AS status_bits_bit_or,
    MAX(reading_ts) AS ended_at,
    PERCENTILE_APPROX(cumulative_pressure, 0.5) AS cumulative_pressure_p50,
    MIN(reading_ts) AS first_reading_ts,
    (UNIX_TIMESTAMP(MAX(reading_ts)) - UNIX_TIMESTAMP(MIN(reading_ts))) / 60 AS duration_min,
    COUNT(*) AS cnt
FROM metrics
GROUP BY DATE_TRUNC('DAY', reading_ts)
ORDER BY cumulative_pressure_p50 DESC;
