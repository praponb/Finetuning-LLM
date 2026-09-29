SELECT
    id,
    store_id,
    quantity,
    product_id,
    FIRST_VALUE(product_id) OVER (PARTITION BY store_id ORDER BY quantity DESC, id) first_product_id
FROM inventory
