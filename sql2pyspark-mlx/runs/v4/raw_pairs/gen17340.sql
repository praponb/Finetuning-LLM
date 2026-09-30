SELECT
    id,
    order_id,
    unit_price,
    quantity,
    unit_price - LAG(unit_price, 2, 0) OVER (PARTITION BY order_id ORDER BY unit_price, id) AS unit_price_change,
    ROUND(AVG(quantity) OVER (ORDER BY unit_price, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS quantity_moving_avg_7
FROM order_items
ORDER BY quantity_moving_avg_7 ASC, id DESC;
