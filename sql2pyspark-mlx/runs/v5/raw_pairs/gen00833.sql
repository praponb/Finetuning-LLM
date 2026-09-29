SELECT country, SUM(rating) AS total_rating, COUNT(*) AS cnt, RANK() OVER (ORDER BY SUM(rating) DESC) AS rnk FROM suppliers GROUP BY country;
