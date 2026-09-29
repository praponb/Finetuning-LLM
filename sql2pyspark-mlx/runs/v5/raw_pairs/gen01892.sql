SELECT
    p.channel,
    p.status AS order_status,
    COUNT(*) AS num_rows,
    MAX(e.cost) AS max_cost
FROM shipments e
LEFT JOIN orders p ON e.order_id = p.id
GROUP BY GROUPING SETS ((p.channel, p.status), (p.channel), ())
ORDER BY max_cost NULLS LAST
