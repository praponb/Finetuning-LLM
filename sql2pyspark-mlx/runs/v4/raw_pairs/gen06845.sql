SELECT country, SUM(rating) AS total_rating, SUM(SUM(rating)) OVER (ORDER BY country) AS cumulative_rating
FROM suppliers
GROUP BY country
ORDER BY cumulative_rating DESC, country
