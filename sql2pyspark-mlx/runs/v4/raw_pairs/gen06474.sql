SELECT
    id,
    store_id,
    product_id,
    last_restock,
    ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY last_restock, id) AS row_num
FROM inventory
