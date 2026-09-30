SELECT sale_date, store_id FROM store_sales WHERE units BETWEEN 20 AND 26 EXCEPT SELECT sale_date, store_id FROM store_sales WHERE store_id IN (2, 3, 4, 5) ORDER BY store_id DESC;
