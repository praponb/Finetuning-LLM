SELECT COUNT(CASE WHEN country = 'JP' THEN 1 END) AS jp_count, MAX(credit_limit) - MIN(credit_limit) AS credit_limit_range
FROM customers
ORDER BY jp_count DESC
