SELECT
    id,
    quantity,
    unit_price,
    SUM(unit_price) OVER (PARTITION BY order_id, product_id) AS order_id_sum_unit_price,
    LEAD(unit_price, 1, 0) OVER w_ordered AS next_unit_price
FROM order_items
WINDOW w_ordered AS (PARTITION BY product_id ORDER BY quantity, id)
