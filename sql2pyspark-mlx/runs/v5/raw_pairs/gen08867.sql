WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, TRIM(country) AS clean_country
    FROM sessions
    WHERE started_at > TO_TIMESTAMP('2023-04-05 05:00:00')
      AND country IS NOT NULL
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.web_event_page
    FROM filtered s
    JOIN LATERAL (
            SELECT c.page AS web_event_page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
        )
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) > INTERVAL '7' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM kept
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        web_event_page,
        SUM(session_start_flag) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM sessionized s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.started_at >= sub.start_date
       AND s.started_at <= DATE_ADD(sub.end_date, 7)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN monthly_fee >= 249 THEN 'LARGE' ELSE 'SMALL' END AS monthly_fee_tier,
    DATE_TRUNC('WEEK', started_at) AS week_start,
    country,
    SORT_ARRAY(COLLECT_SET(device)) AS device_list,
    PERCENTILE_APPROX(burst_id, 0.95) AS burst_id_p95,
    COUNT(CASE WHEN web_event_page = '/cart' THEN 1 END) AS cart_count
FROM attributed
GROUP BY 1, 2, 3
