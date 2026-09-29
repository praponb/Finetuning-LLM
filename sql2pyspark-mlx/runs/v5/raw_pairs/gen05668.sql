select order_date, 'order' as origin from orders where store_id is not null union select last_restock, 'inventory' as origin from inventory
