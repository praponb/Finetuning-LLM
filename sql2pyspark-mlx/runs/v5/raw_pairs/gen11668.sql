SELECT c.is_active, COUNT(*) AS row_count FROM sessions s LEFT JOIN customers c ON s.customer_id = c.id WHERE c.age > 65 GROUP BY c.is_active ORDER BY row_count DESC;
