SELECT id, cost, status FROM shipments WHERE cost >= (SELECT MAX(cost) FROM shipments);
