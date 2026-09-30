SELECT id, product_id, store_id, DATE_TRUNC('week', last_restock) AS last_restock_period, ROUND(quantity, 2) AS quantity_rounded FROM inventory WHERE product_id IN (4, 10) AND quantity <= 204;
