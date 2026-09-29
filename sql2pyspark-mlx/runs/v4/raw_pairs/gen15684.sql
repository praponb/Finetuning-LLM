WITH cleaned AS (
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        pressure,
        firmware,
        LENGTH(firmware) AS firmware_length
    FROM device_readings
    WHERE HOUR(reading_ts) BETWEEN 12 AND 20
)
SELECT
    *,
    AVG(humidity) OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS humidity_avg_6,
    ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY firmware_length DESC, id) AS event_seq,
    ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity, id) AS seq_no
FROM cleaned
