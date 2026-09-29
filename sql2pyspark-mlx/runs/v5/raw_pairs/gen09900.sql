SELECT SUM(CASE WHEN country = 'DE' THEN 1 ELSE 0 END) AS de_count FROM customers WHERE country = 'DE' ORDER BY de_count;
