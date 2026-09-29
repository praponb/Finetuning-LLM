select id, product_id, quantity, rank() over (partition by product_id order by quantity) as rnk from order_items where order_id in (12, 21, 24);
