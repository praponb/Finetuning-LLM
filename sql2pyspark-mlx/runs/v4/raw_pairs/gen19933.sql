SELECT *, DATE_TRUNC('month', last_restock) AS last_restock_period FROM inventory WHERE store_id IN (1, 3, 4, 6) AND product_id = 4 AND store_id IN (2, 5);
