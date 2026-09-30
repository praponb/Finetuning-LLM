select id, store_id, last_restock, quantity, lead(quantity, 2) over (partition by store_id order by last_restock, id) as next_quantity from inventory order by last_restock, id desc;
