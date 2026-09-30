SELECT
    id,
    location,
    budget,
    dept_name,
    LAG(budget, 1) OVER (PARTITION BY location ORDER BY budget, id) AS prev_budget,
    DENSE_RANK() OVER (PARTITION BY dept_name ORDER BY budget DESC) AS dense_rnk
FROM departments
WHERE budget < 3650000;
