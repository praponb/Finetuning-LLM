SELECT id, segment, credit_limit
FROM customers
WHERE country = 'UK' AND (city NOT IN ('Singapore', 'Bangkok') OR is_active);
