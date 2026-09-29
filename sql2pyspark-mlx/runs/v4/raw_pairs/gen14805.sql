SELECT
    id,
    dept_name,
    location,
    budget,
    ROUND(AVG(budget) OVER (PARTITION BY dept_name, location ORDER BY budget, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS budget_moving_avg_3,
    LEAD(budget, 2, 0) OVER (PARTITION BY dept_name ORDER BY budget, id) AS next_budget
FROM departments
WHERE dept_name = 'Finance'
