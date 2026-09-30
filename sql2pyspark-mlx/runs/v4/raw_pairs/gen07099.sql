SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity DESC NULLS LAST, id) AS dedup_rank
    FROM device_readings
) x
WHERE dedup_rank = 1
