SELECT *
FROM store_sales
WHERE revenue IS NOT NULL AND units BETWEEN 14 AND 15 AND product_id IN (1, 3, 5, 7)
