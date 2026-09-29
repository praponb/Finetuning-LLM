WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, status_bits + pressure AS status_bits_plus_pressure
    FROM device_readings
    WHERE reading_ts >= TIMESTAMP '2025-09-01 02:00:00'
      AND humidity >= 75
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(temp_c) OVER (PARTITION BY device_code ORDER BY humidity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_temp_c,
        RANK() OVER (PARTITION BY device_code ORDER BY status_bits_plus_pressure DESC NULLS FIRST) AS status_bits_plus_pressure_rank,
        NTILE(4) OVER (PARTITION BY device_code ORDER BY status_bits DESC, id ASC) AS status_bits_bucket
    FROM valid
),
matched_keys AS (
    -- Set intersection on keys
    SELECT device_code
    FROM metrics
    INTERSECT
    SELECT device_code
    FROM device_readings
    WHERE TO_DATE(reading_ts) < '2025-02-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN matched_keys k ON s.device_code = k.device_code
)
-- Grouped metrics
SELECT
    CASE WHEN running_temp_c >= 148 THEN 'HIGH' ELSE 'LOW' END AS running_temp_c_tier,
    device_code,
    SUM(CASE WHEN device_code = 'DV-02' THEN humidity ELSE 0 END) AS dv_02_humidity,
    MAX(reading_ts) AS ended_at
FROM reconciled
GROUP BY CUBE (CASE WHEN running_temp_c >= 148 THEN 'HIGH' ELSE 'LOW' END, device_code)
HAVING dv_02_humidity > 0
ORDER BY running_temp_c_tier ASC NULLS LAST
