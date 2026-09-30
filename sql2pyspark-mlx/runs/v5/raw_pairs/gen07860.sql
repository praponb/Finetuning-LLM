SELECT id, last_restock, ROW_NUMBER() OVER (ORDER BY last_restock, id) rn FROM inventory;
