SELECT status, MAX(discount) AS max_discount FROM orders GROUP BY status ORDER BY max_discount, status LIMIT 5
