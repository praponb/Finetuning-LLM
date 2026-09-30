SELECT customer_id, SUM(monthly_fee) total_monthly_fee FROM subscriptions GROUP BY customer_id HAVING total_monthly_fee >= 410 ORDER BY total_monthly_fee DESC;
