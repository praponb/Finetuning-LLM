WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, DATE_FORMAT(reading_ts, 'yyyy-MM-dd') AS reading_ts_label
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c DESC NULLS FIRST, id) AS latest_rank
    FROM filtered
    QUALIFY latest_rank = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(humidity) OVER (PARTITION BY device_code) AS device_code_min_humidity,
        CORR(humidity, temp_c) OVER (PARTITION BY device_code) AS corr_humidity_temp_c
    FROM deduped
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        status_bits,
        firmware,
        reading_ts_label,
        latest_rank,
        device_code_min_humidity,
        corr_humidity_temp_c,
        TO_DATE(reading_ts) AS reading_ts_date,
        CASE WHEN corr_humidity_temp_c >= 0.41 THEN 'High' WHEN corr_humidity_temp_c >= -0.44 THEN 'Medium' ELSE 'Low' END AS corr_humidity_temp_c_level
    FROM scored
    WHERE reading_ts >= TO_TIMESTAMP('2025-09-01 04:00:00')
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, INITCAP(TRIM(device_code)) AS device_code_title
    FROM valid
    WHERE HOUR(reading_ts) BETWEEN 7 AND 15
)
SELECT
    id,
    device_code,
    temp_c,
    pressure,
    status_bits,
    firmware,
    latest_rank,
    device_code_min_humidity,
    corr_humidity_temp_c
FROM cleaned
ORDER BY device_code_min_humidity
