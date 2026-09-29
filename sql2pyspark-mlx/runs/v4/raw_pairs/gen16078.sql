SELECT
    id,
    dept_name,
    location,
    budget,
    LEAD(budget, 1) OVER (PARTITION BY dept_name, location ORDER BY budget, id) AS next_budget
FROM departments
ORDER BY location DESC, budget DESC
