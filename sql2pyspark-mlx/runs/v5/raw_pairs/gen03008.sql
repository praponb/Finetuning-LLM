SELECT s.id, s.city, s.region, s.opened_date FROM stores s WHERE s.opened_date >= '2023-10-01' AND s.id IN (SELECT store_id FROM orders);
