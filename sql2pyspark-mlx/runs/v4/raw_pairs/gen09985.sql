SELECT
    id,
    store_id,
    product_id,
    quantity,
    last_restock,
    AVG(quantity) OVER (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) quantity_avg_3,
    FIRST_VALUE(quantity, TRUE) OVER (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_quantity
FROM inventory;
