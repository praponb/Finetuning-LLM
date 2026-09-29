SELECT
    id,
    product_id,
    quantity,
    COALESCE(quantity, 0) AS quantity_filled
FROM order_items
WHERE quantity < 6
ORDER BY product_id DESC;
