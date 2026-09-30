SELECT
    p.status AS order_status,
    a.status AS shipment_status,
    COUNT(*) AS num_rows,
    MAX(a.cost) AS max_cost
FROM shipments a
INNER JOIN orders p ON a.order_id = p.id
GROUP BY GROUPING SETS ((p.status, a.status), (p.status))
ORDER BY max_cost ASC;
