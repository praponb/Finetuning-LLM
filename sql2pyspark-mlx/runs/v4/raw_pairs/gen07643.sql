select id, product_id, quantity, ntile(10) over (partition by product_id order by quantity desc, id) as quartile from order_items;
