WITH cleaned AS (
    SELECT
        id,
        device_code,
        pressure,
        status_bits,
        firmware,
        humidity * temp_c AS humidity_times_temp_c
    FROM device_readings
    WHERE reading_ts > TIMESTAMP '2025-09-01 15:00:00'
      AND TO_DATE(reading_ts) >= '2025-09-21'
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure DESC, id) AS latest_rank
    FROM cleaned
    QUALIFY latest_rank = 1
),
enriched AS (
    SELECT
        *,
        CORR(pressure, status_bits) OVER (PARTITION BY device_code) AS corr_pressure_status_bits,
        ROW_NUMBER() OVER (ORDER BY pressure, id) AS seq_no
    FROM latest
),
filtered AS (
    SELECT
        id,
        pressure,
        status_bits,
        firmware,
        humidity_times_temp_c,
        corr_pressure_status_bits,
        ROUND(corr_pressure_status_bits * 1.1, 2) AS corr_pressure_status_bits_scaled,
        FLOOR(seq_no) AS seq_no_whole
    FROM enriched
    WHERE pressure >= 1022
),
enriched_device_readings AS (
    SELECT
        *,
        SUM(corr_pressure_status_bits) OVER w_recent AS running_corr_pressure_status_bits,
        LEAD(corr_pressure_status_bits_scaled, 2) OVER (ORDER BY corr_pressure_status_bits_scaled, id) AS next_corr_pressure_status_bits_scaled,
        DENSE_RANK() OVER (ORDER BY pressure NULLS FIRST) AS pressure_dense_rank,
        SUM(corr_pressure_status_bits_scaled) OVER w_recent AS cumulative_corr_pressure_status_bits_scaled
    FROM filtered
    WINDOW w_recent AS (ORDER BY corr_pressure_status_bits_scaled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    SELECT
        *,
        AVG(status_bits) OVER (ORDER BY status_bits, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS status_bits_avg_6,
        LAG(corr_pressure_status_bits, 2) OVER (ORDER BY status_bits, id) AS prev_corr_pressure_status_bits,
        LAST_VALUE(humidity_times_temp_c, TRUE) OVER (ORDER BY seq_no_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_humidity_times_temp_c
    FROM enriched_device_readings
)
SELECT
    id,
    status_bits,
    humidity_times_temp_c,
    corr_pressure_status_bits,
    corr_pressure_status_bits_scaled,
    running_corr_pressure_status_bits,
    next_corr_pressure_status_bits_scaled,
    pressure_dense_rank,
    cumulative_corr_pressure_status_bits_scaled
FROM scored;
