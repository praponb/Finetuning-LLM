SELECT id, product_id, sale_date, DENSE_RANK() OVER (PARTITION BY product_id ORDER BY sale_date) AS sale_date_dense_rank FROM store_sales WHERE product_id IN (4, 5, 10);
