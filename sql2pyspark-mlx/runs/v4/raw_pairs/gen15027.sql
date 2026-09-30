SELECT id, brand, cost, DENSE_RANK() OVER (PARTITION BY brand ORDER BY cost) AS cost_dense_rank FROM products WHERE ARRAY_CONTAINS(tags, 'new');
