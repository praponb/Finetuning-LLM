SELECT
    id,
    product_id,
    unit_price,
    LEAD(unit_price, 1) OVER (PARTITION BY product_id ORDER BY unit_price, id) AS next_unit_price
FROM order_items
