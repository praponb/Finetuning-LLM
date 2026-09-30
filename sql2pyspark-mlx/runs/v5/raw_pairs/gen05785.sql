SELECT s.* FROM customers s WHERE EXISTS (SELECT 1 FROM orders ch WHERE ch.customer_id = s.id AND ch.status = 'COMPLETED')
