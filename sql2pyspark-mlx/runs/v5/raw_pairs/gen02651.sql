WITH filtered AS (
    SELECT *, CEIL(status_bits) status_bits_whole
    FROM device_readings
    WHERE reading_ts >= TIMESTAMP '2025-09-01 05:00:00'
),
gaps AS (
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 1 HOUR THEN 1 ELSE 0 END AS is_new_session
    FROM filtered
),
sessions_device_readings AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY session_no DESC, id) AS rank_in_group
        FROM sessions_device_readings
    ) ranked
    WHERE rank_in_group = 1
),
valid AS (
    SELECT *, ROUND(humidity, 0) humidity_rounded
    FROM ranked
    WHERE status_bits_whole IS NULL
),
with_gaps AS (
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL '1' HOUR THEN 1 ELSE 0 END AS new_group_flag
    FROM valid
),
sessionized AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
SELECT
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    CASE WHEN temp_c >= 38.4 THEN 'LARGE' ELSE 'SMALL' END AS temp_c_tier,
    ROUND(SUM(session_no) / NULLIF(COUNT(CASE WHEN device_code = 'DV-02' THEN 1 END), 0), 2) AS session_no_per_dv_02,
    STDDEV_SAMP(temp_c) AS temp_c_stddev,
    SUM(session_no) AS total_session_no,
    PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('HOUR', reading_ts) ORDER BY ROUND(SUM(session_no) / NULLIF(COUNT(CASE WHEN device_code = 'DV-02' THEN 1 END), 0), 2) DESC) AS session_no_per_dv_02_percent_rank
FROM sessionized
GROUP BY DATE_TRUNC('HOUR', reading_ts), CASE WHEN temp_c >= 38.4 THEN 'LARGE' ELSE 'SMALL' END
HAVING SUM(session_no) > 0
ORDER BY total_session_no DESC;
