SELECT
    id,
    product_id,
    unit_price,
    LAG(unit_price, 1) OVER (PARTITION BY product_id ORDER BY unit_price, id) AS prev_unit_price
FROM order_items;
