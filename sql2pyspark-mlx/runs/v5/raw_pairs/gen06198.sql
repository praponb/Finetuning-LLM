SELECT id, rating, supplier_name, country FROM suppliers WHERE rating >= (SELECT AVG(rating) FROM suppliers) ORDER BY country;
