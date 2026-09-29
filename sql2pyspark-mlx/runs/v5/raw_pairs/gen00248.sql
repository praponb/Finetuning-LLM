SELECT
    id,
    order_id,
    quantity,
    product_id,
    LAST_VALUE(product_id) OVER (PARTITION BY order_id ORDER BY quantity DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_product_id
FROM order_items
WHERE product_id IS NOT NULL
