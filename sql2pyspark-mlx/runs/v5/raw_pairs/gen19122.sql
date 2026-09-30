SELECT id, customer_id, plan, end_date, RANK() OVER (PARTITION BY customer_id, plan ORDER BY end_date) AS end_date_rank FROM subscriptions;
