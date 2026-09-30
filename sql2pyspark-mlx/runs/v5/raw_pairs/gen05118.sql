SELECT * FROM (SELECT order_id, carrier, cost FROM shipments)
PIVOT (SUM(cost) FOR carrier IN ('FedEx', 'Kerry', 'DHL', 'Flash'))
ORDER BY order_id;
