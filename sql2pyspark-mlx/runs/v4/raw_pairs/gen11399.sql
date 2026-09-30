SELECT id, city, age, DENSE_RANK() OVER (PARTITION BY city ORDER BY age) AS age_dense_rank FROM customers ORDER BY age DESC
