SELECT id, rating FROM suppliers WHERE rating < (SELECT MIN(rating) FROM suppliers);
