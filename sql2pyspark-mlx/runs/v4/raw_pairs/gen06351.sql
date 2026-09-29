select product_id, quantity from inventory where store_id in (2, 4, 6) union select product_id, quantity from inventory where store_id in (1, 3, 4, 6) order by product_id;
