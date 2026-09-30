WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, LENGTH(device) AS device_length
    FROM sessions
    WHERE country = 'SG'
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM cleaned
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(session_start_flag) OVER (PARTITION BY country ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS session_start_flag_sum_90d,
        AVG(device_length) OVER (PARTITION BY country, browser ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS device_length_avg_7d
    FROM sessions_sessions
)
-- Grouped metrics
SELECT
    CASE WHEN session_no >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS session_no_tier,
    PERCENTILE_APPROX(session_start_flag, ARRAY(0.25, 0.5, 0.75)) AS session_start_flag_quartiles,
    COUNT(CASE WHEN country = 'TH' THEN 1 END) AS th_count,
    ROUND(AVG(session_start_flag), 2) AS avg_session_start_flag,
    SIZE(COLLECT_SET(country)) AS num_countries
FROM with_windows
GROUP BY CASE WHEN session_no >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING SIZE(COLLECT_SET(country)) >= 1
ORDER BY th_count;
