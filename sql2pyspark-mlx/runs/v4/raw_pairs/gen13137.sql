SELECT id, store_id FROM inventory WHERE MONTH(last_restock) IN (3, 6, 11) AND last_restock < '2024-08-01'
