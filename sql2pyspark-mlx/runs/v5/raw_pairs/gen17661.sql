SELECT segment, CAST(is_active AS INT) AS is_active_flag
FROM customers
WHERE city IS NOT NULL AND email NOT LIKE '%@yahoo.com'
ORDER BY is_active_flag, segment
