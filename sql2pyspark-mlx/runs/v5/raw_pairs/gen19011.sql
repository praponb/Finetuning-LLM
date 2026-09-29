SELECT
    id,
    store_id,
    product_id,
    quantity,
    DAYOFWEEK(last_restock) AS last_restock_dayofweek
FROM inventory;
