SELECT id, city, rating, RANK() OVER (PARTITION BY city ORDER BY rating) AS rating_rank FROM suppliers WHERE supplier_name LIKE 'G%' ORDER BY rating DESC, city ASC
