SELECT id, store_id, sale_date, product_id, FIRST_VALUE(product_id) OVER (PARTITION BY store_id ORDER BY sale_date, id) AS first_product_id FROM store_sales ORDER BY first_product_id
