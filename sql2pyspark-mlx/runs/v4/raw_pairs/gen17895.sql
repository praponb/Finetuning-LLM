select order_id, sum(unit_price) as total_unit_price, sum(sum(unit_price)) over (order by order_id) as cumulative_unit_price from order_items where product_id in (24, 36, 38) group by order_id
