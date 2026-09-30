SELECT CASE WHEN rating >= 3.5 THEN 'high' WHEN rating >= 1.4 THEN 'mid' ELSE 'low' END AS rating_bucket, city, COUNT(CASE WHEN city = 'Singapore' THEN 1 END) AS singapore_count
FROM suppliers
GROUP BY CASE WHEN rating >= 3.5 THEN 'high' WHEN rating >= 1.4 THEN 'mid' ELSE 'low' END, city
HAVING singapore_count >= 3;
