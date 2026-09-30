select order_id, avg(quantity) as avg_quantity from order_items where order_id = 36 and quantity is not null group by order_id order by 1 desc, avg_quantity desc limit 10;
