SELECT id, store_id, quantity, last_restock FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity, id) AS row_num FROM inventory) AS ranked WHERE row_num <= 2;
