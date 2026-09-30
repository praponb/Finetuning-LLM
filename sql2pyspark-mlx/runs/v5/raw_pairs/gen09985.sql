SELECT s.id, s.store_name FROM stores s WHERE s.opened_date <= '2024-11-01' AND s.id IN (SELECT store_id FROM orders) ORDER BY 1 DESC, s.store_name DESC
