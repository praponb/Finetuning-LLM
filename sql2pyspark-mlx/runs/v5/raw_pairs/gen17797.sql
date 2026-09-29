SELECT AVG(rating) AS avg_rating, MAX(rating) - MIN(rating) AS rating_range, MAX(rating) AS max_rating FROM suppliers ORDER BY rating_range, max_rating DESC
