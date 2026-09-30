WITH with_gaps AS (
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM sessions
),
islands AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
with_gaps_2 AS (
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM islands
),
sessionized AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        island_id,
        SUM(is_new_session) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps_2
),
matched_keys AS (
    SELECT customer_id
    FROM sessionized
    EXCEPT ALL
    SELECT customer_id
    FROM sessions
    WHERE customer_id = 17
),
kept_rows AS (
    SELECT s.*
    FROM sessionized s
    INNER JOIN matched_keys k ON s.customer_id = k.customer_id
),
kept AS (
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time <= s.started_at
        )
)
SELECT
    p.segment,
    s.browser,
    COUNT(*) AS cnt,
    AVG(s.session_no) AS avg_session_no,
    GROUPING_ID() AS grouping_level
FROM kept s
INNER JOIN customers p ON s.customer_id = p.id
GROUP BY GROUPING SETS ((p.segment, s.browser), (p.segment));
