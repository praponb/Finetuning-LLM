WITH gaps AS (
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) + INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM device_readings
),
sessionized AS (
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        status_bits,
        firmware,
        SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
SELECT
    id,
    device_code,
    reading_ts,
    status_bits,
    firmware,
    burst_id,
    CASE WHEN device_code RLIKE '^(DV-03|DV-04)$' THEN 'EXTENDED' WHEN device_code RLIKE '^(DV-01|DV-02)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS device_code_zone
FROM sessionized;
