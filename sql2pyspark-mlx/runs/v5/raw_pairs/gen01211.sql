SELECT p.region, COUNT(*) AS cnt, AVG(s.quantity) AS avg_quantity, GROUPING_ID() AS gid FROM inventory s LEFT JOIN stores p ON s.store_id = p.id GROUP BY CUBE (p.region)
