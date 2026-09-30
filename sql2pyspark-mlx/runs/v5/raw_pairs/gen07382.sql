SELECT
    id,
    order_id,
    quantity,
    unit_price,
    product_id,
    LAST_VALUE(unit_price) OVER (PARTITION BY order_id ORDER BY quantity DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_unit_price,
    LEAD(quantity, 2) OVER (PARTITION BY product_id ORDER BY quantity, id) next_quantity
FROM order_items
WHERE order_id IN (7, 16);
