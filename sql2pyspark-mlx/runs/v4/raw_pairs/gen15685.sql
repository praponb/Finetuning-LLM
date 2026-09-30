SELECT id, start_date, LENGTH(plan) AS plan_length FROM subscriptions WHERE monthly_fee BETWEEN 70 AND 150 AND customer_id = 27 ORDER BY 1, 2
