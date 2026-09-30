SELECT product_id, MONTH(last_restock) AS last_restock_month, store_id, MAX(last_restock) AS last_last_restock FROM inventory GROUP BY product_id, MONTH(last_restock), store_id;
