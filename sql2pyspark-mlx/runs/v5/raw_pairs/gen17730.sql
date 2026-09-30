WITH ranked AS (SELECT *, RANK() OVER (PARTITION BY location ORDER BY budget) AS rnk FROM departments) SELECT * FROM ranked WHERE rnk <= 3;
