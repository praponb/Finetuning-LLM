WITH flagged AS (
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM sessions
),
sessionized AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
SELECT
    s.id,
    s.customer_id,
    s.device,
    s.browser,
    s.country,
    s.session_start_flag,
    s.burst_id,
    (SELECT SUM(s2.monthly_fee) FROM subscriptions s2 WHERE s2.customer_id = s.customer_id) AS sum_subscription_monthly_fee
FROM sessionized s
ORDER BY id DESC;
