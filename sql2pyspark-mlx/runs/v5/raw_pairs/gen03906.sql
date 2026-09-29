SELECT s.monthly_fee, s.start_date, c.name
FROM subscriptions s
INNER JOIN customers c ON s.customer_id = c.id
WHERE c.name LIKE '%Carol%' AND s.end_date <= '2025-01-01';
