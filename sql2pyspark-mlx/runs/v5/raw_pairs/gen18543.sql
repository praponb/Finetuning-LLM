SELECT id, city, rating FROM (SELECT id, city, rating, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) AS row_num FROM suppliers) AS t WHERE row_num <= 3 ORDER BY city
