SELECT country, SUM(rating) AS total_rating, RANK() OVER (ORDER BY SUM(rating) DESC) AS rating_rank FROM suppliers GROUP BY country ORDER BY country, total_rating;
