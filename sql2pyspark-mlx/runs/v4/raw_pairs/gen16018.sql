SELECT id, quantity, FLOOR(quantity) AS quantity_whole FROM inventory WHERE last_restock >= '2024-05-01' ORDER BY quantity DESC, quantity_whole ASC
