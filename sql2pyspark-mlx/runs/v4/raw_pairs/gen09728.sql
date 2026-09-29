WITH metrics AS (
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
        DENSE_RANK() OVER (PARTITION BY device_code ORDER BY status_bits DESC) AS status_bits_dense_rank,
        CORR(status_bits, pressure) OVER (PARTITION BY device_code) AS corr_status_bits_pressure
    FROM device_readings
),
scored AS (
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
        status_bits_dense_rank,
        corr_status_bits_pressure,
        AVG(status_bits_dense_rank) OVER (PARTITION BY device_code ORDER BY status_bits_dense_rank, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS status_bits_dense_rank_avg_6,
        NTILE(10) OVER (PARTITION BY device_code ORDER BY pressure DESC, id) AS pressure_bucket,
        SUM(status_bits_dense_rank) OVER (ORDER BY status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_bits_dense_rank
    FROM metrics
),
scored_device_readings AS (
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
        status_bits_dense_rank,
        corr_status_bits_pressure,
        status_bits_dense_rank_avg_6,
        pressure_bucket,
        cumulative_status_bits_dense_rank,
        LEAD(humidity, 2) OVER w_hist AS next_humidity,
        ROW_NUMBER() OVER w_recent AS event_seq,
        ROUND(humidity / SUM(humidity) OVER (PARTITION BY device_code), 4) AS humidity_share
    FROM scored
    WINDOW w_hist AS (PARTITION BY device_code ORDER BY status_bits_dense_rank_avg_6, id), w_recent AS (PARTITION BY device_code ORDER BY corr_status_bits_pressure, id)
)
-- Grouped metrics
SELECT
    device_code,
    APPROX_PERCENTILE(status_bits_dense_rank_avg_6, 0.9) AS status_bits_dense_rank_avg_6_p90,
    ROUND(AVG(pressure_bucket), 2) AS avg_pressure_bucket,
    SIZE(COLLECT_SET(device_code)) AS num_device_codes,
    SUM(CASE WHEN device_code = 'DV-04' THEN cumulative_status_bits_dense_rank ELSE 0 END) AS dv_04_cumulative_status_bits_dense_rank,
    MAX(reading_ts) AS ended_at,
    COUNT(*) FILTER (WHERE device_code = 'DV-04') AS dv_04_rows
FROM scored_device_readings
GROUP BY 1
HAVING status_bits_dense_rank_avg_6_p90 > 0
ORDER BY status_bits_dense_rank_avg_6_p90
