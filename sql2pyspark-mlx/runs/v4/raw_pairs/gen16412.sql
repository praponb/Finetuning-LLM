SELECT id, delivered_date FROM shipments WHERE delivered_date = (SELECT MIN(delivered_date) FROM shipments) ORDER BY id LIMIT 10;
