SELECT d.city, COUNT(*) AS cnt, MAX(s.quantity) AS max_quantity FROM inventory s LEFT JOIN stores d ON s.store_id = d.id GROUP BY d.city ORDER BY max_quantity;
