SELECT id, city, rating, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS row_num FROM suppliers;
