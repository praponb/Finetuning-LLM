select store_id, max(quantity) - min(quantity) as quantity_range, max(quantity) as max_quantity from inventory group by 1;
