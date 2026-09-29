WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        status_bits,
        firmware,
        TO_DATE(reading_ts) AS reading_ts_date
    FROM device_readings
    WHERE pressure > 1025
)
-- Window metrics over several different partitions
SELECT
    id,
    device_code,
    reading_ts,
    temp_c,
    firmware,
    LEAD(status_bits) OVER w_grp AS next_status_bits,
    LAG(status_bits) OVER w_grp AS prev_status_bits,
    LEAD(humidity) OVER w_grp AS next_humidity,
    LAG(humidity) OVER w_grp AS prev_humidity,
    ROW_NUMBER() OVER w_grp AS row_seq
FROM valid
WHERE humidity <= 36
WINDOW w_grp AS (PARTITION BY device_code ORDER BY humidity, id);
