SELECT
    id,
    product_id,
    quantity,
    unit_price,
    LAST_VALUE(unit_price) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_price
FROM order_items
WHERE quantity BETWEEN 5 AND 8
ORDER BY last_unit_price DESC;
