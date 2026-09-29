WITH Numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC, id) pos FROM inventory) SELECT id, store_id, quantity, last_restock FROM Numbered WHERE pos <= 2;
