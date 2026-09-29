SELECT id, cost, DENSE_RANK() OVER (ORDER BY cost DESC) cost_dense_rank FROM products WHERE supplier_id = 28 ORDER BY cost_dense_rank DESC;
