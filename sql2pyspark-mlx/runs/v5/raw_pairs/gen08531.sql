SELECT * FROM (SELECT id, location, budget, DENSE_RANK() OVER (PARTITION BY location ORDER BY budget) AS rnk FROM departments) AS r WHERE rnk = 1
