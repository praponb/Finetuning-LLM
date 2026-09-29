SELECT id, quantity, store_id, product_id, LAST_DAY(last_restock) AS last_restock_month_end FROM inventory WHERE YEAR(last_restock) = 2023 ORDER BY last_restock_month_end
