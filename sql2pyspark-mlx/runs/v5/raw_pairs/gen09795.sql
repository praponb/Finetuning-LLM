WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits DESC, id) AS rank_in_group
        FROM device_readings
    ) ranked
    WHERE rank_in_group = 1
)
SELECT
    id,
    device_code,
    humidity,
    firmware,
    rank_in_group,
    CASE WHEN device_code RLIKE '^(DV-04|DV-03)$' THEN 'CORE' WHEN device_code RLIKE '^(DV-01|DV-02)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS device_code_zone
FROM deduped
ORDER BY firmware DESC, device_code_zone DESC
