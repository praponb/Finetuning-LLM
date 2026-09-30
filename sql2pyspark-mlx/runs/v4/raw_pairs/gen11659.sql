select product_id, count(*) as num_inventory, count(*) as cnt from inventory group by product_id
