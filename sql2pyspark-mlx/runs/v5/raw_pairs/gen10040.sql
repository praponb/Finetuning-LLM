WITH deduped AS (SELECT * FROM inventory QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC, id) = 1) SELECT id, quantity, last_restock FROM deduped ORDER BY id;
