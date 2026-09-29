WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(pressure, 0) AS pressure_filled,
        SUBSTRING(firmware, 1, 3) AS firmware_prefix
    FROM device_readings
    WHERE temp_c IS NOT NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) + INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM valid
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(new_group_flag) IGNORE NULLS OVER (PARTITION BY device_code ORDER BY pressure ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_new_group_flag,
        SUM(session_id) OVER w_ordered AS device_code_sum_session_id,
        AVG(pressure_filled) OVER w_ordered AS device_code_avg_pressure_filled
    FROM islands
    WINDOW w_ordered AS (PARTITION BY device_code)
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY device_code_avg_pressure_filled ASC, id ASC) = 1
),
enriched_device_readings AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(new_group_flag) OVER (ORDER BY CAST(reading_ts AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS new_group_flag_sum_30m,
        AVG(status_bits) OVER (ORDER BY session_id ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS status_bits_avg_5,
        CORR(humidity, status_bits) OVER (PARTITION BY device_code) AS corr_humidity_status_bits
    FROM latest
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        STDDEV_SAMP(device_code_avg_pressure_filled) OVER (PARTITION BY device_code) AS device_code_stddev_samp_device_code_avg_pressure_filled,
        PERCENT_RANK() OVER (PARTITION BY device_code ORDER BY device_code_sum_session_id DESC NULLS LAST) AS device_code_sum_session_id_percent_rank
    FROM enriched_device_readings
)
SELECT
    id,
    device_code,
    temp_c,
    humidity,
    status_bits,
    firmware,
    pressure_filled,
    firmware_prefix,
    new_group_flag,
    CASE WHEN device_code RLIKE '^(DV-03|DV-01)$' THEN 'GROUP_A' WHEN device_code RLIKE '^(DV-02|DV-04)$' THEN 'CORE' ELSE 'GROUP_B' END AS device_code_zone
FROM with_windows
ORDER BY pressure_filled DESC
