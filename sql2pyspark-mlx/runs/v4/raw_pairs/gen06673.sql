SELECT
    id,
    budget,
    dept_name,
    location,
    CEIL(budget) AS budget_whole
FROM departments
ORDER BY budget, dept_name
