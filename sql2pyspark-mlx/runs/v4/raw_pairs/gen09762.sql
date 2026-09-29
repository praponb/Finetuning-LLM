SELECT
    id,
    product_id,
    MONTH(last_restock) AS last_restock_month,
    CASE WHEN quantity >= 199 THEN 'High' WHEN quantity >= 91 THEN 'Medium' ELSE 'Low' END AS quantity_level
FROM inventory
WHERE quantity BETWEEN 110 AND 140 AND product_id IN (1, 2)
