SELECT CASE WHEN s.sqft >= 12240 THEN 'high' WHEN s.sqft >= 6324 THEN 'mid' ELSE 'low' END AS sqft_bucket, MIN(i.last_restock) AS first_last_restock
FROM inventory i
INNER JOIN stores s ON s.id = i.store_id
WHERE s.region IN ('South', 'East')
GROUP BY CASE WHEN s.sqft >= 12240 THEN 'high' WHEN s.sqft >= 6324 THEN 'mid' ELSE 'low' END
ORDER BY sqft_bucket DESC
