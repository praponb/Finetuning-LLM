SELECT country, SUM(rating) AS total_rating, DENSE_RANK() OVER (ORDER BY SUM(rating) DESC) AS rnk FROM suppliers GROUP BY country ORDER BY rnk, country;
