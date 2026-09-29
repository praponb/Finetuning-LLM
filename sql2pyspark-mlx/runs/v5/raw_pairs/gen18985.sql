SELECT s.id, s.status, s.cost, s.order_id FROM shipments s WHERE s.cost > (SELECT AVG(s2.cost) FROM shipments s2 WHERE s2.status = s.status);
