SELECT
    id,
    order_id,
    product_id,
    quantity,
    unit_price,
    PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY quantity DESC NULLS FIRST) AS quantity_percent_rank,
    LAG(quantity, 1) OVER (PARTITION BY order_id ORDER BY quantity ASC, id ASC) AS prev_quantity
FROM order_items;
