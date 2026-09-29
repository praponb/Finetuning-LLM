SELECT
    carrier,
    ROUND(STDDEV(cost), 2) AS stddev_cost,
    MAX(cost) AS max_cost,
    MAX(shipped_date) AS latest_shipped_date
FROM shipments
WHERE delivered_date > '2025-07-01' AND order_id IN (20, 37)
GROUP BY carrier
ORDER BY max_cost DESC
