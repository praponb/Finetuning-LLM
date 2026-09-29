select product_id, order_id from order_items where quantity between 3 and 5 union select product_id, order_id from order_items where order_id in (5, 13, 35, 39) order by product_id;
