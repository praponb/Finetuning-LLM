SELECT id, units, store_id, revenue FROM store_sales WHERE product_id IN (1, 3, 9) AND store_id IN (1, 3, 5, 6) ORDER BY store_id DESC, id LIMIT 3;
