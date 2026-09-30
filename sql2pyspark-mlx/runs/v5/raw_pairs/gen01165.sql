WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(temp_c / humidity, 2) AS temp_c_per_humidity
    FROM device_readings
    WHERE reading_ts > TIMESTAMP '2025-09-01 07:00:00'
      AND temp_c <= 37
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        reading_ts,
        temp_c,
        status_bits,
        firmware,
        temp_c_per_humidity,
        AVG(humidity) OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS humidity_avg_5,
        LAST_VALUE(humidity, TRUE) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_humidity,
        LAG(status_bits, 1, 0) OVER (PARTITION BY device_code ORDER BY pressure, id) AS prev_status_bits,
        SUM(status_bits) OVER (PARTITION BY device_code ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_status_bits,
        RANK() OVER (PARTITION BY device_code ORDER BY temp_c DESC NULLS LAST) AS temp_c_rank
    FROM filtered
),
metrics_device_readings AS (
    -- Ranking and gap features
    SELECT
        id,
        reading_ts,
        status_bits,
        temp_c_per_humidity,
        last_known_humidity,
        prev_status_bits,
        running_status_bits,
        MAX(temp_c_rank) OVER (ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_temp_c_rank,
        SUM(temp_c_rank) OVER (ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_temp_c_rank,
        MIN(temp_c_rank) OVER (ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_temp_c_rank,
        AVG(temp_c_rank) OVER (ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_temp_c_rank,
        AVG(last_known_humidity) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS last_known_humidity_avg_2h
    FROM metrics
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(status_bits) OVER w_running AS status_bits_avg_1h,
        ROW_NUMBER() OVER (ORDER BY prev_status_bits DESC, id) AS event_seq
    FROM metrics_device_readings
    WINDOW w_running AS (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(running_min_temp_c_rank) OVER (ORDER BY temp_c_per_humidity, id) AS prev_running_min_temp_c_rank,
        LAG(running_sum_temp_c_rank) OVER (ORDER BY temp_c_per_humidity, id) AS prev_running_sum_temp_c_rank,
        LEAD(running_sum_temp_c_rank) OVER (ORDER BY temp_c_per_humidity, id) AS next_running_sum_temp_c_rank,
        ROW_NUMBER() OVER (ORDER BY temp_c_per_humidity, id) AS seq_no
    FROM with_windows
    WHERE running_max_temp_c_rank BETWEEN 3.2 AND 6.9
)
SELECT
    id,
    reading_ts,
    temp_c_per_humidity,
    last_known_humidity,
    prev_status_bits,
    running_max_temp_c_rank,
    running_sum_temp_c_rank,
    running_min_temp_c_rank,
    running_avg_temp_c_rank
FROM enriched
ORDER BY prev_status_bits;
