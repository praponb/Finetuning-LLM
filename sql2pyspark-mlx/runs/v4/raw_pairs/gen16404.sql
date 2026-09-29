WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_recent seq_no,
        SUM(pressure) OVER w_running running_pressure
    FROM device_readings
    WINDOW w_recent AS (PARTITION BY device_code ORDER BY reading_ts, id), w_running AS (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        seq_no,
        running_pressure,
        LEAD(humidity, 1) OVER (PARTITION BY device_code ORDER BY temp_c, id) next_humidity,
        SUM(humidity) OVER w_part humidity_sum_1h
    FROM enriched
    WINDOW w_part AS (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW)
),
metrics_device_readings AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY pressure) pressure_dense_rank,
        AVG(humidity) OVER (PARTITION BY device_code) device_code_avg_humidity,
        LEAD(humidity, 1) OVER (PARTITION BY device_code ORDER BY next_humidity, id) next_humidity_2,
        COUNT(seq_no) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) seq_no_count_30m
    FROM metrics
),
metrics_device_readings_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY seq_no NULLS LAST) seq_no_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY device_code ORDER BY next_humidity_2 DESC NULLS LAST) next_humidity_2_percent_rank,
        SUM(pressure_dense_rank) OVER (PARTITION BY device_code ORDER BY pressure_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_pressure_dense_rank
    FROM metrics_device_readings
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        seq_no,
        running_pressure,
        next_humidity,
        humidity_sum_1h,
        pressure_dense_rank,
        device_code_avg_humidity,
        next_humidity_2,
        seq_no_count_30m,
        seq_no_dense_rank,
        next_humidity_2_percent_rank,
        running_pressure_dense_rank,
        LEAD(seq_no_count_30m, 2, 0) OVER w_grp next_seq_no_count_30m,
        ROW_NUMBER() OVER (ORDER BY temp_c DESC, id) seq_no_2,
        SUM(seq_no) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) seq_no_sum_2h,
        LAST_VALUE(pressure_dense_rank, TRUE) OVER (PARTITION BY device_code ORDER BY status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_pressure_dense_rank
    FROM metrics_device_readings_2
    WINDOW w_grp AS (PARTITION BY device_code ORDER BY device_code_avg_humidity, id)
)
-- Aggregate with conditional metrics
SELECT
    seq_no,
    SORT_ARRAY(COLLECT_SET(device_code)) AS device_code_list,
    COUNT(CASE WHEN device_code = 'DV-02' THEN 1 END) AS dv_02_count,
    COUNT(*) AS num_rows,
    COUNT_IF(device_code = 'DV-01') AS dv_01_events,
    COUNT(CASE WHEN device_code = 'DV-03' THEN 1 END) AS dv_03_count,
    COUNT(CASE WHEN device_code = 'DV-02' THEN 1 END) AS dv_02_count_2
FROM with_windows
GROUP BY seq_no;
