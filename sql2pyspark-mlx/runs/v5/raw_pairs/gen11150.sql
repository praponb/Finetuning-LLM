SELECT id, city, rating, country, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) AS row_num, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS row_num_2 FROM suppliers
