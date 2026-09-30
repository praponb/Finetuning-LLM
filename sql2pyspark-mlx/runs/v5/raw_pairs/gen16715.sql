SELECT country, COUNT(CASE WHEN city = 'Berlin' THEN 1 END) berlin_count FROM customers GROUP BY 1 HAVING SUM(CASE WHEN segment = 'Corporate' THEN age ELSE 0 END) < 71;
