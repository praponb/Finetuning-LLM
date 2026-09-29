SELECT product_id, store_id, SUM(units) total_units
FROM store_sales
GROUP BY product_id, store_id;
