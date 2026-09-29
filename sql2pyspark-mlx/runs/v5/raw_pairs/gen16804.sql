WITH cleaned AS (
    SELECT
        *,
        ROUND(humidity, 2) AS humidity_rounded,
        CASE WHEN pressure >= 1026 THEN 'High' WHEN pressure >= 988 THEN 'Medium' ELSE 'Low' END AS pressure_band
    FROM device_readings
    WHERE reading_ts > TIMESTAMP '2025-09-01 08:00:00'
),
metrics AS (
    SELECT
        *,
        LAG(humidity) OVER w_part AS prev_humidity,
        LAG(temp_c) OVER w_part AS prev_temp_c,
        LEAD(temp_c) OVER w_part AS next_temp_c,
        ROW_NUMBER() OVER w_part AS seq_no
    FROM cleaned
    WINDOW w_part AS (PARTITION BY device_code ORDER BY humidity ASC, id ASC)
),
latest AS (
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        prev_temp_c
    FROM metrics
    QUALIFY RANK() OVER (PARTITION BY device_code ORDER BY humidity_rounded ASC) = 1
),
scored AS (
    SELECT
        *,
        LEAD(humidity) OVER w_part AS next_humidity,
        LEAD(temp_c) OVER w_part AS next_temp_c,
        ROW_NUMBER() OVER w_part AS event_seq
    FROM latest
    WHERE HOUR(reading_ts) BETWEEN 9 AND 13
    WINDOW w_part AS (ORDER BY prev_temp_c ASC, id ASC)
)
SELECT
    id,
    humidity,
    prev_temp_c,
    next_humidity
FROM scored
ORDER BY id DESC;
