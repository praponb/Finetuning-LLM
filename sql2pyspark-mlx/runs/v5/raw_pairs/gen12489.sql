SELECT
    *,
    SUM(unit_price) OVER (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_unit_price,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price, id) AS event_seq
FROM order_items
