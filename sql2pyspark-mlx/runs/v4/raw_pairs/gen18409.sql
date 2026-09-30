SELECT
    store_id,
    last_restock,
    product_id,
    ROUND(quantity, 1) AS quantity_rounded,
    DATE_TRUNC('MONTH', last_restock) AS last_restock_period
FROM inventory
WHERE store_id IN (1, 6)
