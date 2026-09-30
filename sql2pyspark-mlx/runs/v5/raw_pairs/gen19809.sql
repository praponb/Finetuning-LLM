WITH filtered AS (
    SELECT *, UPPER(firmware) AS firmware_upper
    FROM device_readings
    WHERE pressure BETWEEN 998 AND 1027
)
SELECT
    *,
    ROW_NUMBER() OVER w_part AS seq_no,
    NTILE(5) OVER w_ordered AS status_bits_bucket,
    ROW_NUMBER() OVER w_running AS seq_no_2
FROM filtered
WINDOW w_part AS (ORDER BY humidity DESC, id), w_ordered AS (PARTITION BY device_code ORDER BY status_bits DESC, id), w_running AS (PARTITION BY device_code ORDER BY temp_c, id);
