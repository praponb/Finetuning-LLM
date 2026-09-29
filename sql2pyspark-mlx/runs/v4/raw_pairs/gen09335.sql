select product_id, store_id, max(quantity) as max_quantity, avg(quantity) as avg_quantity from inventory group by product_id, store_id
