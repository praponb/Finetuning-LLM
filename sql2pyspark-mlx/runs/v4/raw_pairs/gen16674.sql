SELECT id, plan, start_date, NTILE(4) OVER (PARTITION BY plan ORDER BY start_date, id) AS bucket FROM subscriptions ORDER BY bucket DESC
