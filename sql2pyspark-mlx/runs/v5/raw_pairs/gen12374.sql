WITH filtered AS (
    SELECT *, ROUND(status_bits * 1.07, 2) AS status_bits_scaled
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
      AND humidity BETWEEN 45 AND 58
)
SELECT
    id,
    device_code,
    humidity,
    pressure,
    firmware,
    status_bits_scaled
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity DESC, id) AS dedup_rank
    FROM filtered
) ranked
WHERE dedup_rank = 1
