WITH remaining_keys AS (
    SELECT id, firmware
    FROM device_readings
    EXCEPT
    SELECT id, firmware
    FROM device_readings
    WHERE TO_DATE(reading_ts) >= '2023-07-15'
),
reconciled AS (
    SELECT s.*
    FROM device_readings s
    JOIN remaining_keys k ON s.id = k.id AND s.firmware = k.firmware
)
SELECT
    device_code,
    CASE WHEN status_bits >= 11 THEN 'LARGE' ELSE 'SMALL' END AS status_bits_tier,
    COUNT(*) AS cnt,
    APPROX_PERCENTILE(status_bits, 0.5) AS status_bits_p50
FROM reconciled
GROUP BY device_code, CASE WHEN status_bits >= 11 THEN 'LARGE' ELSE 'SMALL' END
HAVING COUNT(*) < 1
ORDER BY cnt DESC;
