WITH filtered AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        COALESCE(humidity, 0) AS humidity_filled
    FROM device_readings
    WHERE humidity <= 44
),
enriched AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        humidity_filled,
        SUM(humidity) OVER w_recent AS running_humidity,
        LEAD(humidity, 1, 0) OVER (PARTITION BY device_code ORDER BY humidity, id) AS next_humidity,
        CORR(status_bits, humidity) OVER (PARTITION BY device_code) AS corr_status_bits_humidity,
        MAX(humidity) OVER w_grp AS humidity_max_15m
    FROM filtered
    WINDOW w_recent AS (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW)
),
latest AS (
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits DESC, id) = 1
),
enriched_device_readings AS (
    SELECT
        *,
        SUM(corr_status_bits_humidity) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_corr_status_bits_humidity,
        SUM(humidity_filled) OVER (PARTITION BY device_code ORDER BY running_humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_humidity_filled
    FROM latest
)
SELECT
    id,
    reading_ts,
    temp_c,
    status_bits,
    humidity_filled,
    running_humidity,
    corr_status_bits_humidity,
    humidity_max_15m,
    running_humidity_filled
FROM enriched_device_readings
ORDER BY running_humidity NULLS LAST, reading_ts;
