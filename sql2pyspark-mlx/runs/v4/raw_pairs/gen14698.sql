SELECT
    id,
    quantity,
    product_id,
    store_id
FROM inventory
WHERE quantity >= (SELECT MAX(quantity) FROM inventory);
