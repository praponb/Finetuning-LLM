SELECT CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 2.4 THEN 'mid' ELSE 'low' END AS rating_bucket, MAX(rating) - MIN(rating) AS rating_range, COUNT(rating) AS rating_count
FROM suppliers
WHERE supplier_name NOT LIKE '%Wholesale%'
GROUP BY 1
