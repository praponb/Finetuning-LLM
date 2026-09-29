SELECT s.id, s.store_name, s.region
FROM stores s
WHERE UPPER(TRIM(s.store_name)) = 'AIRPORT KIOSK' AND NOT EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id AND o.discount BETWEEN 0.11 AND 0.19);
