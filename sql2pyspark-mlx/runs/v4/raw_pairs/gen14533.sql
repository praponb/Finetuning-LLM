SELECT COUNT(DISTINCT age) AS unique_segments, COUNT(age) AS age_count FROM customers WHERE credit_limit BETWEEN 10500 AND 13200 ORDER BY unique_segments;
