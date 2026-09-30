SELECT id, country, rating, city, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) rn, RANK() OVER (PARTITION BY city ORDER BY rating DESC) rating_rank FROM suppliers
