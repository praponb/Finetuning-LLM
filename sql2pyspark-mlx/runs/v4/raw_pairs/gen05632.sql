SELECT
    id,
    order_id,
    status,
    CASE WHEN cost >= 83 THEN 'High' WHEN cost >= 39 THEN 'Medium' ELSE 'Low' END AS cost_band
FROM shipments
