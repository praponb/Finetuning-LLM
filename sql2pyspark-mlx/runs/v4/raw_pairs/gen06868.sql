SELECT product_id, CASE WHEN revenue >= 3400 THEN 'High' WHEN revenue >= 800 THEN 'Medium' ELSE 'Low' END AS revenue_tier
FROM store_sales
WHERE store_id = 3 AND (product_id = 5 OR units < 26)
ORDER BY product_id ASC
