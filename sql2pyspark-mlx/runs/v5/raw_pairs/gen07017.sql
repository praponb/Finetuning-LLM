WITH flagged AS (
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM sessions
),
islands AS (
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        country,
        SUM(session_start_flag) OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
SELECT
    s.id,
    s.customer_id,
    s.started_at,
    (SELECT AVG(x.monthly_fee) FROM subscriptions x WHERE x.customer_id = s.customer_id) AS avg_subscription_monthly_fee
FROM islands s
ORDER BY customer_id ASC;
