SELECT
    id,
    cost,
    delivered_date,
    DATEDIFF(CURRENT_DATE(), delivered_date) AS days_since_delivered_date,
    ROUND(cost, 0) AS cost_rounded
FROM shipments
ORDER BY cost DESC, id
LIMIT 20
