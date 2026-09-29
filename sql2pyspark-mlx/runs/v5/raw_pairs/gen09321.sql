SELECT SUM(CASE WHEN city = 'Tokyo' THEN 1 ELSE 0 END) AS tokyo_count FROM suppliers ORDER BY tokyo_count ASC;
