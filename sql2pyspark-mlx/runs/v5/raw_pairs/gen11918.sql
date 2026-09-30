select id, unit_price, dense_rank() over (order by unit_price desc) as dense_rnk from order_items
