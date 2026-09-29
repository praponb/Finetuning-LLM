WITH numbered AS (SELECT *, DENSE_RANK() OVER (PARTITION BY country ORDER BY started_at DESC) rnk FROM sessions) SELECT * FROM numbered WHERE rnk <= 3;
