SELECT sale_date, product_id FROM store_sales WHERE store_id = 3 INTERSECT SELECT sale_date, product_id FROM store_sales WHERE store_id IN (1, 3, 4, 6);
