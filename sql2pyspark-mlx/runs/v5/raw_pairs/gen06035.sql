SELECT *, ROUND(rating / SUM(rating) OVER (PARTITION BY country), 4) AS rating_share, MAX(rating) OVER (PARTITION BY city) AS city_max_rating FROM suppliers;
