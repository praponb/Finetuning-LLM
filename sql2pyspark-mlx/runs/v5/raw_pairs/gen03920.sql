SELECT id, product_id, ROUND(revenue / units, 2) AS revenue_per_unit
FROM store_sales
WHERE product_id IN (4, 5, 6, 9);
