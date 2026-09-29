WITH with_gaps AS (
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM sessions
),
sessions_sessions AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
SELECT
    s.id,
    s.customer_id,
    s.device,
    s.browser,
    s.started_at,
    s.session_id,
    (SELECT MAX(a2.balance) FROM accounts a2 WHERE a2.customer_id = s.customer_id) AS max_account_balance
FROM sessions_sessions s
ORDER BY device, browser;
