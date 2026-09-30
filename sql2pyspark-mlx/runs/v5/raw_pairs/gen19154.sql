SELECT
    id,
    product_id,
    quantity,
    unit_price,
    LAST_VALUE(unit_price) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_price,
    ROW_NUMBER() OVER (ORDER BY quantity, id) AS rn
FROM order_items
