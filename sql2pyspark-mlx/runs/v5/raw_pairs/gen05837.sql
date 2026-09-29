SELECT customer_id, MAX(monthly_fee) AS max_monthly_fee FROM subscriptions GROUP BY customer_id ORDER BY max_monthly_fee DESC
