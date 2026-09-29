SELECT
    CASE WHEN cost >= 86 THEN 'LARGE' ELSE 'SMALL' END AS cost_tier,
    ROUND(SUM(cost) / NULLIF(COUNT(CASE WHEN carrier = 'FedEx' THEN 1 END), 0), 2) AS cost_per_fedex,
    COUNT(DISTINCT order_id) AS distinct_order_ids
FROM shipments
GROUP BY CASE WHEN cost >= 86 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY cost_per_fedex;
