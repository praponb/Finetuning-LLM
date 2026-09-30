SELECT id, store_id, product_id, sale_date, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY sale_date, id) AS rn FROM store_sales WHERE store_id = 4 ORDER BY id DESC, store_id ASC;
