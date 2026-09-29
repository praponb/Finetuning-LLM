WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_SUB(shipped_date, 78) AS shipped_date_minus_78d
    FROM shipments
    WHERE shipped_date BETWEEN '2023-03-06' AND '2024-11-01'
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', shipped_date_minus_78d) AS year_start,
    carrier,
    CASE WHEN cost >= 89 THEN 'HIGH' ELSE 'LOW' END AS cost_tier,
    AVG(cost) AS avg_cost,
    SIZE(COLLECT_SET(status)) AS num_status,
    ROUND(AVG(cost), 2) AS avg_cost_2,
    COUNT_IF(carrier = 'Flash') AS flash_events,
    COUNT_IF(status = 'IN_TRANSIT') AS in_transit_events
FROM filtered
GROUP BY 1, 2, 3
HAVING COUNT_IF(carrier = 'Flash') > 1
ORDER BY in_transit_events DESC NULLS LAST
