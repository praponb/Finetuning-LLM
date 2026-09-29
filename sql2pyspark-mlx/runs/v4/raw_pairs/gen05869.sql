SELECT id, rating, country FROM suppliers WHERE rating >= (SELECT MAX(rating) FROM suppliers WHERE supplier_name IS NOT NULL) ORDER BY id LIMIT 10;
