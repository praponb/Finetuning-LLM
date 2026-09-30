SELECT id, city, rating, NTILE(3) OVER (PARTITION BY city ORDER BY rating, id) bucket, LEAD(rating, 1, 0) OVER (ORDER BY rating, id) next_rating FROM suppliers;
