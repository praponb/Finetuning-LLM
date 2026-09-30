WITH numbered AS (SELECT *, DENSE_RANK() OVER (PARTITION BY status ORDER BY discount) AS pos FROM orders) SELECT id, status, discount FROM numbered WHERE pos <= 2;
