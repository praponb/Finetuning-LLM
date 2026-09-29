WITH prev_events AS (
    SELECT *, LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) AS prev_started_at
    FROM sessions
),
with_gaps AS (
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
sessions_sessions AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        SUM(session_start_flag) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
SELECT
    s.id,
    s.customer_id,
    s.device,
    s.browser,
    s.country,
    CASE WHEN country RLIKE '^(TH|DE)$' THEN 'GROUP_B' WHEN country RLIKE '^(US|SG)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS country_zone,
    (SELECT SUM(x.discount) FROM orders x WHERE x.customer_id = s.customer_id) AS sum_order_discount
FROM sessions_sessions s;
