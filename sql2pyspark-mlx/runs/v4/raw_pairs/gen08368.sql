SELECT
    id,
    unit_price,
    ROUND(AVG(unit_price) OVER (ORDER BY unit_price, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS unit_price_moving_avg_4,
    NTILE(3) OVER (ORDER BY unit_price, id) AS bucket
FROM order_items;
