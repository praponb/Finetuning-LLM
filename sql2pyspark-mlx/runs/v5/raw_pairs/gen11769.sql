WITH gaps AS (
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) + INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM device_readings
),
sessions_device_readings AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        status_bits,
        SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
SELECT
    id,
    device_code,
    temp_c,
    CASE WHEN device_code RLIKE '^(DV-04|DV-01)$' THEN 'EXTENDED' WHEN device_code RLIKE '^(DV-02|DV-03)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS device_code_zone
FROM sessions_device_readings
