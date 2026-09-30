SELECT id, rating, country FROM suppliers WHERE rating > (SELECT AVG(rating) FROM suppliers) ORDER BY id;
