SELECT
    id,
    product_id,
    ROUND(quantity * 0.85, 2) AS quantity_adjusted,
    COALESCE(quantity, 0) AS quantity_filled
FROM order_items
WHERE quantity = 5
ORDER BY quantity_adjusted DESC
