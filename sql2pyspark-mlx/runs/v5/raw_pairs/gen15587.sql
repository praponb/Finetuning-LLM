SELECT
    s.id,
    s.status,
    s.order_id,
    s.delivered_date,
    o.store_id,
    o.customer_id,
    CASE WHEN s.cost >= 74 THEN 'High' WHEN s.cost >= 41 THEN 'Medium' ELSE 'Low' END AS cost_tier
FROM shipments s
FULL OUTER JOIN orders o ON s.order_id = o.id
WHERE o.status IN ('CANCELLED', 'COMPLETED', 'SHIPPED') AND MONTH(s.shipped_date) IN (2, 7, 9)
