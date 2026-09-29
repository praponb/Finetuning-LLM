SELECT id, customer_id
FROM subscriptions
WHERE start_date BETWEEN '2023-10-27' AND '2024-12-30' AND status <> 'PAUSED';
