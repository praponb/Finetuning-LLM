WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_TRUNC('month', started_at) AS started_at_period,
        TO_DATE(started_at) AS started_at_date
    FROM sessions
    WHERE started_at >= TIMESTAMP '2023-12-08 17:00:00'
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM valid
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM sessions_sessions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at_date NULLS LAST, id) = 1
),
summary AS (
    -- Grouped metrics
    SELECT
        CASE WHEN session_id >= 7 THEN 'HIGH' ELSE 'LOW' END AS session_id_tier,
        browser,
        SUM(session_id) FILTER (WHERE device = 'mobile') AS mobile_total_session_id,
        MIN(started_at_period) AS first_seen,
        MAX(session_start_flag) FILTER (WHERE browser = 'Edge') AS edge_max_session_start_flag,
        SIZE(COLLECT_SET(country)) AS num_countries
    FROM deduped
    GROUP BY session_id_tier, browser
    HAVING num_countries < 1
)
SELECT
    session_id_tier,
    browser,
    edge_max_session_start_flag,
    num_countries
FROM summary
ORDER BY session_id_tier;
