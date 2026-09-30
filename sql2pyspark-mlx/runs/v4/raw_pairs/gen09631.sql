WITH cleaned AS (
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        pressure,
        CASE WHEN status_bits >= 11 THEN 'High' WHEN status_bits >= 5 THEN 'Medium' ELSE 'Low' END AS status_bits_tier,
        TO_DATE(reading_ts) AS reading_ts_date
    FROM device_readings
    WHERE reading_ts > TIMESTAMP '2025-09-01 13:00:00'
      AND status_bits > 8
),
base AS (
    SELECT *, CEIL(pressure) AS pressure_whole
    FROM cleaned
    WHERE temp_c >= 38
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c, id) AS rank_in_group
        FROM base
    ) ranked
    WHERE rank_in_group = 1
),
matched_keys AS (
    SELECT id, device_code
    FROM ranked
    EXCEPT ALL
    SELECT id, device_code
    FROM device_readings
    WHERE device_code = 'DV-02'
),
kept_rows AS (
    SELECT s.*
    FROM ranked s
    INNER JOIN matched_keys k ON s.id = k.id AND s.device_code = k.device_code
)
SELECT
    id,
    device_code,
    humidity,
    status_bits_tier,
    pressure_whole,
    CASE WHEN device_code RLIKE '^(DV-04|DV-02)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS device_code_zone
FROM kept_rows
ORDER BY pressure_whole DESC
