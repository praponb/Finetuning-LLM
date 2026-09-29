SELECT id, rating
FROM suppliers
WHERE rating < (SELECT MIN(rating) FROM suppliers WHERE rating <= 2.9)
