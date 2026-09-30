SELECT product_id, MAX(quantity) - MIN(quantity) AS quantity_range
FROM inventory
GROUP BY product_id
