WITH ranked AS (SELECT *, DENSE_RANK() OVER (PARTITION BY city ORDER BY rating DESC) AS rn FROM suppliers) SELECT * FROM ranked WHERE rn <= 2 ORDER BY country
