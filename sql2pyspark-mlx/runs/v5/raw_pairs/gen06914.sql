SELECT
    id,
    dept_name,
    budget,
    location,
    ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name), 3) AS budget_share,
    LAG(budget, 2) OVER (PARTITION BY location ORDER BY budget, id) AS prev_budget
FROM departments
WHERE budget >= 1280000;
