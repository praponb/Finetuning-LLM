SELECT
    id,
    order_id,
    product_id,
    quantity,
    unit_price,
    AVG(quantity) OVER (ORDER BY quantity, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS quantity_avg_6,
    CUME_DIST() OVER (PARTITION BY product_id ORDER BY unit_price) AS unit_price_cume_dist,
    LAST_VALUE(unit_price, TRUE) OVER (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price,
    FIRST_VALUE(quantity, TRUE) OVER (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity
FROM order_items
