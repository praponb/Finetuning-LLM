SELECT
    id,
    store_id,
    last_restock,
    quantity,
    ROUND(AVG(quantity) OVER (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) quantity_moving_avg_7
FROM inventory
ORDER BY id ASC, last_restock ASC
