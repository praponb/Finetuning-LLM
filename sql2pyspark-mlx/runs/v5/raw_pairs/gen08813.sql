SELECT
    carrier,
    order_id,
    COUNT(cost) AS cost_count,
    AVG(cost) AS avg_cost,
    SUM(CASE WHEN carrier = 'DHL' THEN 1 ELSE 0 END) AS dhl_count
FROM shipments
WHERE order_id IS NOT NULL
GROUP BY 1, 2
ORDER BY avg_cost DESC, 1 DESC;
