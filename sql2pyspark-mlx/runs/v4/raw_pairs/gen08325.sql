SELECT
    dept_name,
    COUNT(DISTINCT location) AS distinct_locations,
    ROUND(AVG(budget), 2) AS avg_budget
FROM departments
GROUP BY ROLLUP (dept_name)
