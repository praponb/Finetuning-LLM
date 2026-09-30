SELECT method, COUNT(CASE WHEN status = 'FAILED' THEN 1 END) AS failed_count FROM payments GROUP BY method ORDER BY failed_count DESC;
