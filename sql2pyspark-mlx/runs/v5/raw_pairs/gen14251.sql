SELECT product_id, MAX(last_restock) AS latest_last_restock FROM inventory GROUP BY 1
