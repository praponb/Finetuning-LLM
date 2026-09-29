SELECT method, SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count FROM payments GROUP BY method HAVING AVG(amount) >= 1260 ORDER BY method LIMIT 20;
