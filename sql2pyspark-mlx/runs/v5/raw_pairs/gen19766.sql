SELECT id, country, rating, RANK() OVER (PARTITION BY country ORDER BY rating DESC) AS rating_rank FROM suppliers ORDER BY rating, 4 DESC;
