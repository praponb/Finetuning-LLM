WITH ranked AS (SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY monthly_fee DESC) AS latest_rank FROM subscriptions QUALIFY latest_rank = 1) SELECT id, plan, status, latest_rank FROM ranked
