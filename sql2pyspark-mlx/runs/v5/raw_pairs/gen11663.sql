select id, store_id, quantity, last_restock, round(quantity, 0) as quantity_rounded from inventory order by store_id desc, id limit 1
