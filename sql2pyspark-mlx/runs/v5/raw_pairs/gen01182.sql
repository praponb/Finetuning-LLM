SELECT id, plan, customer_id FROM subscriptions WHERE end_date BETWEEN '2023-07-05' AND '2024-11-01' AND customer_id IS NULL;
