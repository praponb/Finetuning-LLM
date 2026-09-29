SELECT
    id,
    store_id,
    last_restock,
    quantity,
    ROUND(AVG(quantity) OVER (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS quantity_moving_avg_7
FROM inventory
WHERE store_id IN (1, 4, 6);
