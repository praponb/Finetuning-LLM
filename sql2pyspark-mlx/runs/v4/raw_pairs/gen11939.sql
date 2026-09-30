select id, quantity, product_id, unit_price from order_items where quantity >= (select max(quantity) from order_items where order_id = 40) order by quantity desc;
