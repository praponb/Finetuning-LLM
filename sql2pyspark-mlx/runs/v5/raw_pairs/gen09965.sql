SELECT id, rating, LEAD(rating, 1) OVER (ORDER BY rating, id) AS next_rating
FROM suppliers
ORDER BY rating DESC
