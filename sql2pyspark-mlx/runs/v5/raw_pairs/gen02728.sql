WITH numbered AS (SELECT *, DENSE_RANK() OVER (PARTITION BY country ORDER BY rating DESC) AS rn FROM suppliers) SELECT * FROM numbered WHERE rn <= 2 ORDER BY id DESC, rating;
