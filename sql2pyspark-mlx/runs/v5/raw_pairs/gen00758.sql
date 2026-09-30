SELECT
    id,
    device_code,
    humidity,
    pressure,
    firmware,
    LEAD(temp_c) OVER w_running AS next_temp_c,
    LEAD(humidity) OVER w_running AS next_humidity,
    LAG(humidity) OVER w_running AS prev_humidity,
    ROW_NUMBER() OVER w_running AS seq_no
FROM device_readings
WINDOW w_running AS (PARTITION BY device_code ORDER BY pressure, id);
