SELECT
    id,
    order_id,
    unit_price,
    quantity,
    NTILE(2) OVER (PARTITION BY order_id ORDER BY unit_price DESC, id) AS quartile,
    ROUND(AVG(quantity) OVER (ORDER BY unit_price, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS quantity_moving_avg_7
FROM order_items
ORDER BY unit_price;
