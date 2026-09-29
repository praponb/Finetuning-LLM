SELECT method, COUNT(DISTINCT amount) AS unique_statuss, COUNT(CASE WHEN method = 'cash' THEN 1 END) AS cash_count FROM payments GROUP BY 1 ORDER BY method DESC
