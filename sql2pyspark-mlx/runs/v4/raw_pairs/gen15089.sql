SELECT
    id,
    order_id,
    cost,
    CASE WHEN cost >= 100 THEN 'High' WHEN cost >= 38 THEN 'Medium' ELSE 'Low' END AS cost_tier
FROM shipments;
