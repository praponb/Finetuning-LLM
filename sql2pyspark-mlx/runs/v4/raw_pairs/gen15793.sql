WITH valid AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        SUBSTRING(firmware, 1, 2) AS firmware_prefix
    FROM device_readings
    WHERE humidity > 60
),
metrics AS (
    SELECT
        *,
        SUM(humidity) OVER w_ordered AS running_humidity,
        ROW_NUMBER() OVER (ORDER BY temp_c DESC, id) AS row_seq,
        DENSE_RANK() OVER (ORDER BY temp_c DESC) AS temp_c_dense_rank,
        NTILE(10) OVER (PARTITION BY device_code ORDER BY temp_c, id) AS temp_c_bucket
    FROM valid
    WINDOW w_ordered AS (ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics_device_readings AS (
    SELECT
        *,
        SUM(temp_c_bucket) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS temp_c_bucket_sum_1h,
        AVG(humidity) OVER (PARTITION BY device_code ORDER BY status_bits, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS humidity_avg_2_prior,
        NTILE(3) OVER (PARTITION BY device_code ORDER BY temp_c DESC, id) AS temp_c_bucket_2,
        RANK() OVER (PARTITION BY device_code ORDER BY humidity DESC NULLS LAST) AS humidity_rank,
        SUM(temp_c) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_temp_c
    FROM metrics
),
scored AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        firmware_prefix,
        running_humidity,
        row_seq,
        temp_c_dense_rank,
        temp_c_bucket,
        temp_c_bucket_sum_1h,
        humidity_avg_2_prior,
        temp_c_bucket_2,
        humidity_rank,
        cumulative_temp_c,
        LEAD(row_seq, 1) OVER (PARTITION BY device_code ORDER BY running_humidity, id) AS next_row_seq,
        MAX(status_bits) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS status_bits_max_15m
    FROM metrics_device_readings
)
SELECT
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    SUM(CASE WHEN device_code = 'DV-04' THEN humidity ELSE 0 END) AS dv_04_humidity,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(CASE WHEN device_code = 'DV-01' THEN temp_c_bucket_2 ELSE 0 END) AS dv_01_temp_c_bucket_2,
    MIN(reading_ts) AS started_at,
    COUNT(*) FILTER (WHERE device_code = 'DV-04') AS dv_04_rows
FROM scored
GROUP BY DATE_TRUNC('HOUR', reading_ts)
ORDER BY dv_04_rows DESC
