SELECT id, start_date, monthly_fee
FROM subscriptions
WHERE plan NOT IN ('enterprise', 'basic', 'premium') AND end_date BETWEEN '2023-05-18' AND '2025-05-01';
