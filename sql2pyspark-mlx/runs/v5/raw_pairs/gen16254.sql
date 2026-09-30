SELECT * FROM (SELECT order_id, status, cost FROM shipments) src PIVOT (MAX(cost) FOR status IN ('LOST' AS lost, 'IN_TRANSIT' AS in_transit));
