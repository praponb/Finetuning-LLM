WITH RankedSuppliers AS (SELECT id, city, rating, DENSE_RANK() OVER (PARTITION BY city ORDER BY rating) AS rn FROM suppliers) SELECT id, city, rating FROM RankedSuppliers WHERE rn <= 3
