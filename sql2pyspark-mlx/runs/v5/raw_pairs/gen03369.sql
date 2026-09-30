SELECT id, start_date, customer_id, LENGTH(plan) AS plan_length FROM subscriptions WHERE end_date BETWEEN '2025-01-01' AND '2025-05-01';
