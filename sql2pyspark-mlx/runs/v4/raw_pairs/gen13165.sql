SELECT id, delivered_date FROM shipments WHERE delivered_date = (SELECT MAX(delivered_date) FROM shipments WHERE YEAR(shipped_date) >= 2024) ORDER BY delivered_date;
