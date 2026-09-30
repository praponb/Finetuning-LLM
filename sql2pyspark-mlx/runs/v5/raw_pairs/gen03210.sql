SELECT
    product_id,
    COUNT(unit_price) AS unit_price_count,
    ROUND(STDDEV(quantity), 2) AS stddev_quantity,
    SUM(quantity * unit_price) AS revenue
FROM order_items
GROUP BY product_id
HAVING stddev_quantity >= 26;
