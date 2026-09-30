WITH filtered_shipments AS (
    SELECT *
    FROM shipments
    WHERE cost > 32
),
summary AS (
    SELECT carrier, ROUND(STDDEV(cost), 2) AS stddev_cost, MAX(cost) - MIN(cost) AS cost_range
    FROM filtered_shipments
    GROUP BY carrier
)
SELECT carrier, stddev_cost, cost_range
FROM summary
WHERE stddev_cost < 34
ORDER BY cost_range, carrier;
