select store_id, avg(quantity) as avg_quantity, max(quantity) as max_quantity from inventory group by store_id order by store_id desc, avg_quantity desc;
