select product_id, max(quantity) as max_quantity from inventory where store_id in (1, 3) group by product_id
