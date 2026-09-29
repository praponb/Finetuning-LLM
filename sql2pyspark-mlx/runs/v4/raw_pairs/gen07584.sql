SELECT
    o.status,
    QUARTER(o.order_date) AS order_quarter,
    ROUND(AVG(oi.unit_price), 2) AS avg_unit_price,
    MAX(oi.quantity) - MIN(oi.quantity) AS quantity_range
FROM order_items oi
INNER JOIN orders o ON o.id = oi.order_id
LEFT OUTER JOIN products p ON oi.product_id = p.id
WHERE o.discount >= 0.29
GROUP BY o.status, QUARTER(o.order_date)
ORDER BY avg_unit_price, status, order_quarter
LIMIT 5
