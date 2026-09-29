SELECT
    id,
    dept_name,
    budget,
    location,
    RANK() OVER (PARTITION BY dept_name ORDER BY budget) AS budget_rank,
    LEAD(budget, 1) OVER (PARTITION BY location ORDER BY budget, id) AS next_budget
FROM departments
ORDER BY location, 1
