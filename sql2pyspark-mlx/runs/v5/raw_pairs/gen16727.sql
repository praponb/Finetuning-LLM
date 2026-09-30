SELECT CASE WHEN s.sqft >= 17580 THEN 'high' WHEN s.sqft >= 6174 THEN 'mid' ELSE 'low' END AS sqft_bucket, COUNT(*) AS total_count
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
INNER JOIN stores s ON ss.store_id = s.id
GROUP BY CASE WHEN s.sqft >= 17580 THEN 'high' WHEN s.sqft >= 6174 THEN 'mid' ELSE 'low' END
HAVING total_count >= 2
ORDER BY sqft_bucket, total_count DESC;
