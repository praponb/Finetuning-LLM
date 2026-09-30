SELECT * FROM (SELECT id, city, rating, country, DENSE_RANK() OVER (PARTITION BY city ORDER BY rating) AS row_num FROM suppliers) AS r WHERE row_num <= 3;
