SELECT id, rating, rating - LAG(rating, 1) OVER (ORDER BY rating, id) AS rating_change FROM suppliers WHERE rating <= 2.2 ORDER BY rating_change DESC, rating
