SELECT id, rating, ROW_NUMBER() OVER (ORDER BY rating DESC, id) AS rating_row FROM suppliers WHERE city IN ('Berlin', 'Chiang Mai', 'Tokyo');
