SELECT id, plan, end_date, RANK() OVER (PARTITION BY plan ORDER BY end_date) AS rnk FROM subscriptions ORDER BY plan, end_date;
