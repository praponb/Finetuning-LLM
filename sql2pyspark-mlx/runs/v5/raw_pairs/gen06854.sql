WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM web_events
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        SUM(is_new_session) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
-- Multi-level aggregation
SELECT
    page,
    CASE WHEN burst_id >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS burst_id_tier,
    SUM(burst_id) FILTER (WHERE event_type = 'purchase') AS purchase_total_burst_id,
    COUNT(DISTINCT event_type) AS distinct_event_types,
    APPROX_PERCENTILE(burst_id, ARRAY(0.25, 0.5, 0.75)) AS burst_id_quartiles,
    APPROX_PERCENTILE(burst_id, ARRAY(0.25, 0.5, 0.75)) AS burst_id_quartiles_2,
    SORT_ARRAY(COLLECT_SET(page)) AS page_list
FROM sessions_web_events
GROUP BY page, CASE WHEN burst_id >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING SUM(burst_id) FILTER (WHERE event_type = 'purchase') > 0;
