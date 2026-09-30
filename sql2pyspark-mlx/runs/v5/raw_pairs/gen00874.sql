SELECT
    location,
    dept_name,
    MAX(budget) - MIN(budget) AS budget_range,
    SUM(budget) AS total_budget
FROM departments
WHERE budget <= 2537000
GROUP BY location, dept_name
HAVING SUM(CASE WHEN location = 'New York' THEN 1 ELSE 0 END) >= 45
