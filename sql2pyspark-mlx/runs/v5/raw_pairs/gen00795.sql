SELECT s.id, s.status, s.cost, s.carrier FROM shipments s WHERE s.cost = (SELECT MIN(s2.cost) FROM shipments s2 WHERE s2.status = s.status)
