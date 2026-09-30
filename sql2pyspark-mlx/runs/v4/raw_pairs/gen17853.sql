WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, pressure + humidity AS pressure_plus_humidity
    FROM device_readings
    WHERE device_code != 'DV-02'
)
-- Window metrics over several different partitions
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c DESC, id) AS row_seq,
    SUM(humidity) OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_humidity
FROM cleaned;
