select id, store_id, last_restock, quantity, quantity - lag(quantity, 1) over (partition by store_id order by last_restock, id) quantity_change from inventory;
