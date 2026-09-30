SELECT
    id,
    status,
    delivered_date,
    CASE WHEN status = 'LOST' THEN 1 ELSE 0 END AS is_lost,
    ROUND(cost * 0.85, 2) AS cost_adjusted
FROM shipments
WHERE cost <= 96 AND status = 'RETURNED' AND shipped_date >= '2023-12-03';
