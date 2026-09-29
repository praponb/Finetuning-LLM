SELECT id, plan, start_date, end_date FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY end_date DESC, id) AS rn FROM subscriptions) x WHERE rn = 1
