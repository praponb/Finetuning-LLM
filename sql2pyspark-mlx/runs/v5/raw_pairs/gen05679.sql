SELECT id, rating, DENSE_RANK() OVER (ORDER BY rating DESC) AS rating_dense_rank FROM suppliers ORDER BY rating_dense_rank, rating DESC
