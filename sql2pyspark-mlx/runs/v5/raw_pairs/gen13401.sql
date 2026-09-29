WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_TRUNC('WEEK', reading_ts) AS reading_ts_period,
        CONCAT(firmware, ' - ', device_code) AS firmware_device_code
    FROM device_readings
    WHERE device_code != 'DV-01'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS row_num
    FROM filtered
    QUALIFY row_num = 1
),
deduped_device_readings AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure NULLS FIRST, id) AS dedup_rank
        FROM deduped
    ) ranked
    WHERE dedup_rank = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(pressure) OVER w_grp AS last7_avg_pressure,
        MAX(pressure) OVER w_grp AS last7_max_pressure,
        MIN(pressure) OVER w_grp AS last7_min_pressure,
        CORR(status_bits, humidity) OVER (PARTITION BY device_code) AS corr_status_bits_humidity
    FROM deduped_device_readings
    WINDOW w_grp AS (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM with_windows
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        reading_ts_period,
        firmware_device_code,
        row_num,
        dedup_rank,
        last7_max_pressure,
        last7_min_pressure,
        SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(last7_max_pressure, TRUE) OVER (ORDER BY row_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last7_max_pressure,
        PERCENT_RANK() OVER w_grp AS pressure_percent_rank,
        AVG(last7_max_pressure) OVER (ORDER BY pressure, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS last7_max_pressure_avg_5
    FROM sessions_device_readings
    WINDOW w_grp AS (PARTITION BY device_code ORDER BY pressure DESC NULLS LAST)
)
SELECT
    id,
    device_code,
    temp_c,
    reading_ts_period,
    firmware_device_code,
    row_num,
    dedup_rank,
    last7_max_pressure,
    island_id,
    CASE WHEN device_code RLIKE '^(DV-02|DV-01)$' THEN 'CORE' WHEN device_code RLIKE '^(DV-03|DV-04)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS device_code_zone
FROM enriched
ORDER BY temp_c DESC;
