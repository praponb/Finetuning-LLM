SELECT s.id, s.status, s.cost FROM shipments s WHERE s.cost = (SELECT MAX(s2.cost) FROM shipments s2 WHERE s2.status = s.status) ORDER BY s.status, s.id DESC
