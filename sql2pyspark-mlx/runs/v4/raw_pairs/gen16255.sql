WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(pressure, 1) AS pressure_rounded,
        DATEDIFF(CURRENT_DATE(), reading_ts) AS days_since_reading_ts
    FROM device_readings
    WHERE humidity > 43
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c DESC NULLS LAST, id) AS latest_rank
        FROM cleaned
    ) t
    WHERE latest_rank = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, UPPER(firmware) AS firmware_upper
    FROM deduped
    WHERE reading_ts > TO_TIMESTAMP('2025-09-01 11:00:00')
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(pressure_rounded) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_pressure_rounded,
        AVG(temp_c) OVER (ORDER BY reading_ts, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS temp_c_avg_6_prior,
        AVG(humidity) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS humidity_avg_3,
        MAX(pressure_rounded) OVER w_running AS pressure_rounded_max_30m,
        AVG(status_bits) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS status_bits_avg_15m
    FROM filtered
    WINDOW w_running AS (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW)
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM scored
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
kept_keys AS (
    -- Set difference on keys
    SELECT device_code, firmware
    FROM sessions_device_readings
    EXCEPT
    SELECT device_code, firmware
    FROM device_readings
    WHERE status_bits < 7
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessions_device_readings s
    JOIN kept_keys k ON s.device_code = k.device_code AND s.firmware = k.firmware
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        firmware,
        device_code,
        humidity_avg_3
    FROM reconciled
) src
PIVOT (
    SUM(humidity_avg_3)
    FOR device_code IN ('DV-04' AS dv_04, 'DV-02' AS dv_02, 'DV-03' AS dv_03)
)
