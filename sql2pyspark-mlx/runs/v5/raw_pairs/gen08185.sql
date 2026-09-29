SELECT s.id, s.status, s.cost, s.shipped_date FROM shipments s WHERE s.cost > (SELECT AVG(s2.cost) FROM shipments s2 WHERE s2.status = s.status) ORDER BY s.id;
