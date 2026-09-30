SELECT s.id, s.monthly_fee, c.is_active
FROM subscriptions s
LEFT JOIN customers c ON s.customer_id = c.id
ORDER BY monthly_fee, id DESC;
