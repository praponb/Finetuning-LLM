SELECT
    id,
    dept_name,
    budget,
    SUM(budget) OVER (PARTITION BY dept_name) AS dept_name_sum_budget
FROM departments
WHERE location <> 'Chiang Mai'
ORDER BY dept_name_sum_budget, id DESC;
