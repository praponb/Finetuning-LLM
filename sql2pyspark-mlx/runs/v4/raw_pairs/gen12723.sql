WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM sessions
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
SELECT
    s.id,
    s.customer_id,
    s.device,
    s.browser,
    s.started_at,
    s.country,
    CASE WHEN device RLIKE '^(tablet|desktop)$' THEN 'GROUP_A' WHEN device RLIKE '^(mobile)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS device_zone,
    (SELECT MAX(t2.balance) FROM accounts t2 WHERE t2.customer_id = s.customer_id) AS max_account_balance
FROM islands s
ORDER BY device_zone DESC
