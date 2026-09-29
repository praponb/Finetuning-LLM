SELECT
    id,
    store_id,
    last_restock,
    DATE_TRUNC('YEAR', last_restock) AS last_restock_period,
    COALESCE(quantity, 0) AS quantity_filled
FROM inventory
WHERE last_restock < '2023-11-23' AND product_id = 3
