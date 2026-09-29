SELECT s.id, s.opened_date FROM stores s WHERE s.id IN (SELECT store_id FROM inventory WHERE product_id = 3) ORDER BY s.id DESC LIMIT 1
