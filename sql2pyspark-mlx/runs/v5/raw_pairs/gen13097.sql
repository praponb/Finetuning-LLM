SELECT
    *,
    NTILE(4) OVER (ORDER BY quantity, id) AS quantity_bucket,
    MAX(quantity) OVER (PARTITION BY order_id, product_id) AS order_id_max_quantity
FROM order_items
WHERE product_id IS NULL
