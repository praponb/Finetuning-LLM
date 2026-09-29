SELECT
    *,
    LEAD(budget, 1) OVER (PARTITION BY location ORDER BY budget, id) AS next_budget,
    MAX(budget) OVER (PARTITION BY dept_name, location) AS dept_name_max_budget
FROM departments
