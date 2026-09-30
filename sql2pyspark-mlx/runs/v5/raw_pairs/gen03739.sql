SELECT last_restock, product_id, quantity, LAST_DAY(last_restock) AS last_restock_month_end FROM inventory WHERE quantity >= 167 AND last_restock <= '2024-05-01';
