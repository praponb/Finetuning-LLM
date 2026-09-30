select store_id, max(last_restock) as last_last_restock from inventory group by store_id;
