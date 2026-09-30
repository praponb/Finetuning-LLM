SELECT id, store_id, quantity, RANK() OVER (PARTITION BY store_id ORDER BY quantity DESC) AS quantity_rank FROM inventory WHERE last_restock < '2024-12-01';
