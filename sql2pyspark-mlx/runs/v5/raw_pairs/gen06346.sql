SELECT
    id,
    product_id,
    quantity,
    store_id,
    RANK() OVER (PARTITION BY product_id ORDER BY quantity) AS rnk,
    ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity, id) AS quantity_row
FROM inventory
WHERE quantity = 176
ORDER BY product_id DESC;
