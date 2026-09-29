SELECT CASE WHEN rating >= 4.5 THEN 'high' WHEN rating >= 1.4 THEN 'mid' ELSE 'low' END AS rating_bucket, city, SUM(CASE WHEN country = 'TH' THEN rating ELSE 0 END) AS th_rating
FROM suppliers
WHERE supplier_name LIKE 'Sia%'
GROUP BY 1, 2
ORDER BY city, rating_bucket
LIMIT 5;
