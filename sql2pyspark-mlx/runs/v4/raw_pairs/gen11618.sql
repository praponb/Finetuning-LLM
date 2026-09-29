SELECT
    YEAR(last_restock) last_restock_year,
    product_id,
    store_id,
    MIN(last_restock) min_last_restock,
    MIN(quantity) min_quantity,
    MAX(quantity) - MIN(quantity) quantity_range
FROM inventory
WHERE store_id = 3
GROUP BY YEAR(last_restock), product_id, store_id;
