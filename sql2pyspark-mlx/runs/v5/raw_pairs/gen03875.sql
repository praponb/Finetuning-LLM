SELECT id, amount, DENSE_RANK() OVER (ORDER BY amount DESC) amount_dense_rank FROM payments ORDER BY amount_dense_rank
