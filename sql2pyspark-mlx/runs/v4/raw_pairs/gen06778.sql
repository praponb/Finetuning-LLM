SELECT last_restock, product_id, ROUND(quantity * 1.1, 2) AS quantity_adjusted FROM inventory WHERE quantity = 165 OR store_id = 1 ORDER BY quantity_adjusted DESC, last_restock DESC
