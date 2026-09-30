SELECT order_id, ROUND((100 * SUM(CASE WHEN carrier = 'UPS' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_ups, SUM(CASE WHEN carrier = 'Kerry' THEN cost ELSE 0 END) kerry_cost
FROM shipments
GROUP BY order_id
