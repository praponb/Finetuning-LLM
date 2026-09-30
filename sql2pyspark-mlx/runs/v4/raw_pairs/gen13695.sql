SELECT
    product_id,
    APPROX_PERCENTILE(unit_price, ARRAY(0.25, 0.5, 0.75)) AS unit_price_quartiles,
    SUM(quantity) AS total_quantity,
    CORR(quantity, unit_price) AS corr_quantity_unit_price,
    ROUND(AVG(unit_price), 2) AS avg_unit_price,
    MAX(quantity) AS max_quantity,
    COUNT(*) AS event_count
FROM order_items
GROUP BY product_id
ORDER BY event_count
