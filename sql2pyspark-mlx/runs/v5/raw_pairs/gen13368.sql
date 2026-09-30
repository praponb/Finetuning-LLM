SELECT id, store_id, quantity, DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock FROM inventory
