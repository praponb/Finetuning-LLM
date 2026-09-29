WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DAY(reading_ts) AS reading_ts_day
    FROM device_readings
    WHERE reading_ts >= TIMESTAMP '2025-09-01 05:00:00'
)
-- Ranking and gap features
SELECT
    *,
    AVG(reading_ts_day) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS reading_ts_day_avg_1h,
    FIRST_VALUE(temp_c, TRUE) OVER (PARTITION BY device_code ORDER BY status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_temp_c
FROM valid;
