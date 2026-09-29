SELECT country, rating_count FROM (SELECT country, COUNT(rating) AS rating_count FROM suppliers WHERE country NOT IN ('SG', 'JP') GROUP BY country) AS agg WHERE rating_count < 8 ORDER BY 2
