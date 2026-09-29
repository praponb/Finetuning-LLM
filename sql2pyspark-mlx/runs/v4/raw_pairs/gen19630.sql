SELECT country, SUM(rating) AS total_rating, COUNT(*) AS cnt, DENSE_RANK() OVER (ORDER BY SUM(rating) DESC) AS rating_rank FROM suppliers WHERE supplier_name IS NOT NULL GROUP BY country;
