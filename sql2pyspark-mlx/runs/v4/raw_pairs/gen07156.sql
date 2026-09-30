SELECT
    id,
    unit_price,
    quantity,
    product_id,
    LEAD(quantity, 1, 0) OVER (ORDER BY unit_price, id) AS next_quantity,
    LAST_VALUE(unit_price) OVER (PARTITION BY product_id ORDER BY quantity DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_price
FROM order_items
WHERE order_id IN (18, 25, 29, 35)
ORDER BY unit_price DESC
