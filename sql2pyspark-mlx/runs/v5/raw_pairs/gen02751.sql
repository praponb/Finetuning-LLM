SELECT id, product_id, last_restock, LEAD(quantity) OVER w_grp AS next_quantity, ROW_NUMBER() OVER w_grp AS row_seq FROM inventory WINDOW w_grp AS (PARTITION BY store_id ORDER BY last_restock, id);
