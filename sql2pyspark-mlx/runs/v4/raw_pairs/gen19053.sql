SELECT *, LAG(rating, 2) OVER (PARTITION BY country ORDER BY rating, id) AS prev_rating, ROUND(rating / SUM(rating) OVER (PARTITION BY country), 4) AS rating_share FROM suppliers;
