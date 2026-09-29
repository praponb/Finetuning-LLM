WITH recent_shipments AS (
    SELECT *
    FROM shipments
    WHERE status NOT IN ('LOST', 'IN_TRANSIT')
),
summary AS (
    SELECT
        status,
        YEAR(delivered_date) AS delivered_year,
        MAX(cost) - MIN(cost) AS cost_range,
        ROUND(AVG(cost), 2) AS avg_cost
    FROM recent_shipments
    GROUP BY status, YEAR(delivered_date)
)
SELECT *
FROM summary
WHERE avg_cost >= 28
ORDER BY status DESC, cost_range DESC
