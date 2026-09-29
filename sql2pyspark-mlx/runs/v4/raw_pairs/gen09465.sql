WITH with_gaps AS (
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '1' HOUR THEN 1 ELSE 0 END AS session_start_flag
    FROM device_readings
),
islands AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
with_windows AS (
    SELECT
        *,
        LAST_VALUE(temp_c) IGNORE NULLS OVER w_recent AS last_known_temp_c,
        SUM(humidity) OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_humidity,
        RANK() OVER (PARTITION BY device_code ORDER BY humidity DESC NULLS LAST) AS humidity_rank
    FROM islands
    WINDOW w_recent AS (PARTITION BY device_code ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered AS (
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        session_start_flag,
        burst_id,
        last_known_temp_c,
        cumulative_humidity,
        CONCAT(firmware, ' - ', device_code) AS firmware_device_code
    FROM with_windows
    WHERE session_start_flag = 1
),
totals AS (
    SELECT
        CASE WHEN burst_id >= 9 THEN 'LARGE' ELSE 'SMALL' END AS burst_id_tier,
        SIZE(COLLECT_SET(device_code)) AS num_device_codes,
        COUNT_IF(device_code = 'DV-02') AS dv_02_events,
        COUNT_IF(device_code = 'DV-03') AS dv_03_events
    FROM filtered
    GROUP BY ROLLUP (CASE WHEN burst_id >= 9 THEN 'LARGE' ELSE 'SMALL' END)
    HAVING num_device_codes > 1
)
SELECT
    burst_id_tier,
    num_device_codes,
    dv_02_events,
    dv_03_events
FROM totals
