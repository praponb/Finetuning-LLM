select id, product_id, store_id from store_sales where month(sale_date) in (2, 3, 9) union select id, product_id, store_id from store_sales where product_id in (7, 9);
