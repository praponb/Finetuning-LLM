SELECT id, customer_id, end_date, NTILE(2) OVER (PARTITION BY customer_id ORDER BY end_date, id) AS end_date_ntile FROM subscriptions WHERE status NOT IN ('CANCELLED', 'PAUSED')
