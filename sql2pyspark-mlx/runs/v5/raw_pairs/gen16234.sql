SELECT a.id, a.status, a.cost FROM shipments a WHERE a.cost < (SELECT AVG(b.cost) FROM shipments b WHERE b.status = a.status)
