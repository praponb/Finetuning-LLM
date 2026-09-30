SELECT CASE WHEN s.sqft >= 13347 THEN 'high' WHEN s.sqft >= 7598 THEN 'mid' ELSE 'low' END AS sqft_bucket, MAX(i.quantity) AS max_quantity, MIN(i.last_restock) AS min_last_restock
FROM inventory i
LEFT JOIN products p ON i.product_id = p.id
INNER JOIN stores s ON i.store_id = s.id
GROUP BY CASE WHEN s.sqft >= 13347 THEN 'high' WHEN s.sqft >= 7598 THEN 'mid' ELSE 'low' END;
