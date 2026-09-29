WITH enriched AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity DESC, id) AS seq_no,
        CORR(pressure, temp_c) OVER (PARTITION BY device_code) AS corr_pressure_temp_c,
        AVG(status_bits) OVER (ORDER BY status_bits, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS status_bits_avg_6_prior,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits DESC, id) AS event_seq
    FROM device_readings
)
SELECT
    id,
    reading_ts,
    temp_c,
    humidity,
    pressure,
    status_bits,
    status_bits_avg_6_prior,
    event_seq
FROM enriched
ORDER BY humidity, temp_c DESC;
