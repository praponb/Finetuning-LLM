SELECT a.status, c.country, c.email
FROM accounts a
LEFT JOIN customers c ON c.id = a.customer_id
WHERE c.is_active = TRUE AND c.age < 46;
