SELECT id, rating, supplier_name FROM suppliers WHERE rating >= (SELECT AVG(rating) FROM suppliers) ORDER BY id
