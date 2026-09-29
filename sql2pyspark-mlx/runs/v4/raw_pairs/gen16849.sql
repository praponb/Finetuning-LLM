SELECT name, is_active, credit_limit
FROM customers
WHERE country IN ('TH', 'SG', 'JP');
