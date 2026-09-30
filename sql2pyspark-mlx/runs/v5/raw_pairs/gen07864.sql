select id, order_id, quantity from order_items where product_id is null union all select id, order_id, quantity from order_items where quantity between 4 and 6;
