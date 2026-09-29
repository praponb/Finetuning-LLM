SELECT s.id, s.region, s.store_name FROM stores s WHERE s.region IN ('South', 'East') AND s.id IN (SELECT store_id FROM inventory) ORDER BY s.id DESC
