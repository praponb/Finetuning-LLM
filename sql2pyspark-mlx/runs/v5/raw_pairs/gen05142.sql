SELECT id, customer_id, monthly_fee, status FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee, id) AS rn FROM subscriptions) AS ranked WHERE rn <= 5
