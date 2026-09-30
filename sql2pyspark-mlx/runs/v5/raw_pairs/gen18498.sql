SELECT
    id,
    order_id,
    LEAD(quantity, 1, 0) OVER (ORDER BY unit_price, id) AS next_quantity,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price, id) AS seq_no,
    FIRST_VALUE(unit_price, TRUE) OVER (PARTITION BY product_id, order_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price,
    ROUND(quantity / SUM(quantity) OVER (PARTITION BY order_id), 4) AS quantity_share
FROM order_items
