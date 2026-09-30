SELECT id, rating, LEAD(rating, 1, 0) OVER (ORDER BY rating, id) AS next_rating
FROM suppliers
WHERE supplier_name LIKE 'T%';
