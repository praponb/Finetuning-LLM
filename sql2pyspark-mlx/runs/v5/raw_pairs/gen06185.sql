WITH filtered AS (
    SELECT
        *,
        INITCAP(TRIM(device_code)) device_code_title,
        SUBSTRING(device_code, 1, 2) device_code_prefix
    FROM device_readings
    WHERE reading_ts > TO_DATE('2025-09-01')
),
enriched AS (
    SELECT
        *,
        SUM(temp_c) OVER w_hist running_sum_temp_c,
        AVG(temp_c) OVER w_hist running_avg_temp_c,
        COUNT(temp_c) OVER w_hist running_count_temp_c,
        DENSE_RANK() OVER w_ordered pressure_dense_rank
    FROM filtered
    WINDOW w_hist AS (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (ORDER BY pressure)
),
remaining_keys AS (
    SELECT id, device_code
    FROM enriched
    EXCEPT
    SELECT id, device_code
    FROM device_readings
    WHERE device_code NOT IN ('DV-03', 'DV-04', 'DV-02')
),
kept_rows AS (
    SELECT s.*
    FROM enriched s
    JOIN remaining_keys k ON s.id = k.id AND s.device_code = k.device_code
),
metrics AS (
    SELECT
        device_code,
        reading_ts,
        status_bits,
        device_code_title,
        device_code_prefix,
        running_sum_temp_c,
        running_count_temp_c,
        pressure_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY device_code ORDER BY pressure_dense_rank DESC NULLS FIRST) pressure_dense_rank_percent_rank,
        AVG(pressure_dense_rank) OVER w_hist device_code_avg_pressure_dense_rank,
        STDDEV_SAMP(running_count_temp_c) OVER w_hist device_code_stddev_samp_running_count_temp_c
    FROM kept_rows
    WINDOW w_hist AS (PARTITION BY device_code)
)
SELECT
    device_code,
    reading_ts,
    status_bits,
    device_code_title,
    running_count_temp_c,
    pressure_dense_rank,
    pressure_dense_rank_percent_rank,
    device_code_avg_pressure_dense_rank,
    device_code_stddev_samp_running_count_temp_c
FROM metrics
ORDER BY pressure_dense_rank DESC;
