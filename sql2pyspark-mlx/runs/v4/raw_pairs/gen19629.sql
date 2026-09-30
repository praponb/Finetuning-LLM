SELECT id, rating, country, supplier_name FROM suppliers WHERE rating >= (SELECT AVG(rating) FROM suppliers) ORDER BY country DESC, id LIMIT 50;
