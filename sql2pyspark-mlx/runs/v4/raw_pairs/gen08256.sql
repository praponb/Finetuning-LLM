SELECT
    id,
    store_id,
    product_id,
    quantity,
    last_restock,
    ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY quantity, id) AS quantity_row,
    SUM(quantity) OVER (PARTITION BY product_id ORDER BY last_restock, id) AS running_quantity
FROM inventory;
