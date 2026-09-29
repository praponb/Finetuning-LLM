select product_id, sum(quantity) as total_quantity, count(*) as cnt, sum(sum(quantity)) over (order by product_id) as cumulative_quantity from order_items where quantity = 4 group by product_id
