SELECT city, COUNT(CASE WHEN country = 'TH' THEN 1 END) AS th_count, AVG(rating) AS avg_rating
FROM suppliers
GROUP BY city
HAVING AVG(rating) >= 2.9
ORDER BY th_count DESC;
