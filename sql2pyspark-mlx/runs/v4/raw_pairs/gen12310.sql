select product_id, order_id, count(*) as num_order_items from order_items where unit_price between 230 and 400 group by 1, 2 order by 2 desc
