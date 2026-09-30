SELECT *, CASE WHEN quantity >= 209 THEN 'High' WHEN quantity >= 80 THEN 'Medium' ELSE 'Low' END AS quantity_level, DATE_TRUNC('YEAR', last_restock) AS last_restock_period
FROM inventory
WHERE product_id IN (5, 6, 10) AND quantity >= 116 AND product_id = 10;
