SELECT segment, COUNT(age) AS age_count, COUNT(credit_limit) AS credit_limit_count
FROM customers
WHERE is_active = TRUE
GROUP BY segment;
