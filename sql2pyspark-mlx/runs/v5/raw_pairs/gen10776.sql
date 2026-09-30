SELECT id, rating, LEAD(rating, 2) OVER (ORDER BY rating, id) next_rating
FROM suppliers
WHERE city IS NULL;
