WITH filtered AS (
    SELECT
        *,
        LENGTH(country) AS country_length,
        COALESCE(browser, 'UNKNOWN') AS browser_clean
    FROM sessions
    WHERE customer_id IS NOT NULL
      AND country NOT IN ('DE', 'SG')
),
flagged AS (
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM filtered
),
islands AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
prev_events AS (
    SELECT *, LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) AS prev_started_at
    FROM islands
),
with_gaps AS (
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL '7' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
sessionized AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
latest_match AS (
    SELECT
        s.*,
        x.duration_sec,
        x.web_event_event_type
    FROM sessionized s
    LEFT JOIN LATERAL (
            SELECT c.duration_sec, c.event_type AS web_event_event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT *
FROM (
    SELECT
        browser,
        device,
        country,
        country_length
    FROM latest_match
) src
PIVOT (
    AVG(country_length) AS value_sum,
    COUNT(1) AS n
    FOR country IN ('US', 'JP')
);
