select id, store_id, product_id, revenue from store_sales where revenue > 1600 and units < 36 order by id desc, 3;
