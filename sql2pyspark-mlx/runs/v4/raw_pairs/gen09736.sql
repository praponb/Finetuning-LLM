SELECT *, ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share, NTILE(4) OVER (PARTITION BY country ORDER BY rating, id) AS rating_bucket FROM suppliers
