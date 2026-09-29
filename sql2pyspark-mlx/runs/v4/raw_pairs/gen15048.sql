select product_id, round(avg(quantity), 2) avg_quantity, max(quantity) max_quantity, max(quantity) - min(quantity) quantity_range from inventory group by product_id
