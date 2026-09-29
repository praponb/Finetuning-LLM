select id, unit_price, dense_rank() over (order by unit_price desc) as unit_price_dense_rank from order_items where order_id in (1, 5, 13);
