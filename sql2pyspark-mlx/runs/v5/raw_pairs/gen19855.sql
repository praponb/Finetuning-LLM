SELECT id, product_id, quantity, store_id, LAST_DAY(last_restock) AS last_restock_month_end FROM inventory WHERE product_id IN (1, 7) AND product_id IN (8, 9) ORDER BY store_id ASC
