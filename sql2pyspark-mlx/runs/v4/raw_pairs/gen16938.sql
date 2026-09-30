SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY quantity DESC, id) AS event_seq,
    ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY order_id), 4) AS unit_price_share,
    LAST_VALUE(quantity) IGNORE NULLS OVER (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity,
    SUM(unit_price) OVER (PARTITION BY order_id, product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_unit_price,
    LAST_VALUE(unit_price, TRUE) OVER (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price
FROM order_items
