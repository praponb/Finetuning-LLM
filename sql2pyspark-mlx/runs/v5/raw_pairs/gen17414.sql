SELECT
    id,
    store_id,
    last_restock,
    product_id,
    DATE_TRUNC('WEEK', last_restock) AS last_restock_period,
    CAST(quantity AS DOUBLE) AS quantity_num
FROM inventory
