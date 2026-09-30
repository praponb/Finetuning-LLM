SELECT
    order_id,
    status,
    CASE WHEN carrier IN ('Flash', 'Kerry') THEN 'priority' ELSE 'other' END AS carrier_group,
    MIN(cost) AS min_cost,
    ROUND(STDDEV(cost), 2) AS stddev_cost,
    SUM(CASE WHEN carrier = 'FedEx' THEN 1 ELSE 0 END) AS fedex_count
FROM shipments
GROUP BY order_id, status, carrier_group
ORDER BY stddev_cost DESC;
