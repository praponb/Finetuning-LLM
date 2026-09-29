WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        status_bits,
        firmware,
        CASE WHEN device_code = 'DV-01' THEN 1 ELSE 0 END AS is_dv_01
    FROM device_readings
    WHERE device_code = 'DV-02'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        status_bits,
        firmware,
        is_dv_01,
        PERCENT_RANK() OVER (ORDER BY is_dv_01 DESC NULLS LAST) AS is_dv_01_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY reading_ts DESC, id ASC) AS event_seq,
        SUM(pressure) OVER (PARTITION BY device_code ORDER BY is_dv_01 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_pressure
    FROM cleaned
),
matched_keys AS (
    -- Set intersection on keys
    SELECT device_code
    FROM metrics
    INTERSECT
    SELECT device_code
    FROM device_readings
    WHERE humidity <= 63
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    JOIN matched_keys k ON s.device_code = k.device_code
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        status_bits,
        firmware,
        is_dv_01_percent_rank,
        event_seq,
        running_pressure,
        CASE WHEN device_code = 'DV-04' THEN 1 ELSE 0 END AS is_dv_04
    FROM reconciled
    WHERE reading_ts >= TO_DATE('2025-09-01')
      AND running_pressure BETWEEN 4050 AND 4159
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(is_dv_04, 1) AS is_dv_04_rounded,
        is_dv_04 * running_pressure AS is_dv_04_times_running_pressure
    FROM valid
    WHERE device_code != 'DV-04'
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        device_code,
        reading_ts,
        temp_c,
        status_bits,
        firmware,
        is_dv_01_percent_rank,
        event_seq,
        is_dv_04,
        is_dv_04_times_running_pressure,
        CASE WHEN pressure >= 1017 THEN 'High' WHEN pressure >= 1004 THEN 'Medium' ELSE 'Low' END AS pressure_band
    FROM filtered
    WHERE reading_ts > DATE '2025-09-01'
      AND device_code = 'DV-03'
)
-- Grouped metrics
SELECT
    CASE WHEN status_bits >= 9 THEN 'HIGH' ELSE 'LOW' END AS status_bits_tier,
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    AVG(is_dv_04) FILTER (WHERE device_code = 'DV-02') AS dv_02_avg_is_dv_04,
    SUM(CASE WHEN device_code = 'DV-02' THEN is_dv_01_percent_rank ELSE 0 END) AS dv_02_is_dv_01_percent_rank,
    AVG(event_seq) FILTER (WHERE device_code = 'DV-03') AS dv_03_avg_event_seq
FROM base
GROUP BY CASE WHEN status_bits >= 9 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('DAY', reading_ts)
