SELECT id, category, cost, price, RANK() OVER (PARTITION BY category ORDER BY cost) AS rnk, DENSE_RANK() OVER (PARTITION BY category ORDER BY price) AS price_dense_rank FROM products;
