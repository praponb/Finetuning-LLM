SELECT order_id, MAX(cost) AS max_cost, COUNT(*) AS cnt
FROM shipments
WHERE carrier IN ('UPS', 'Flash', 'DHL') AND carrier = 'DHL'
GROUP BY order_id
ORDER BY max_cost DESC;
