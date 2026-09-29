SELECT product_id, store_id, COUNT(*) AS store_sale_count
FROM store_sales
WHERE store_id IN (2, 4) AND sale_date >= '2024-05-08' AND units = 24
GROUP BY product_id, store_id
