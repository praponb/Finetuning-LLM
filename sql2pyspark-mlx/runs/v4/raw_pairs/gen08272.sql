SELECT
    id,
    product_id,
    quantity,
    order_id,
    CEIL(quantity) AS quantity_whole
FROM order_items
WHERE product_id = 17 OR product_id IS NULL
ORDER BY order_id ASC, id ASC
LIMIT 10
