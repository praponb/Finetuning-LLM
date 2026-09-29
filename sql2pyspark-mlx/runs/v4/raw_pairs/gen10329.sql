SELECT id, customer_id, plan, monthly_fee, RANK() OVER (PARTITION BY customer_id, plan ORDER BY monthly_fee DESC) AS monthly_fee_rank FROM subscriptions
