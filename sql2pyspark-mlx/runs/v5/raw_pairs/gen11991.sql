SELECT id, status, end_date, PERCENT_RANK() OVER (PARTITION BY status ORDER BY end_date) AS end_date_pct_rank FROM subscriptions
