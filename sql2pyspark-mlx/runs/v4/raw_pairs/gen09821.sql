SELECT i.quantity, s.region, UPPER(s.region) AS region_upper
FROM inventory i
JOIN stores s ON i.store_id = s.id;
