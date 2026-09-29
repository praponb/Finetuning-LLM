SELECT
    o.order_date,
    o.amount,
    s.cost,
    s.status,
    FLOOR(s.cost) AS cost_whole
FROM orders o
LEFT JOIN shipments s ON s.order_id = o.id
WHERE o.order_date > '2023-03-26' OR s.order_id IS NOT NULL
ORDER BY 5 DESC;
