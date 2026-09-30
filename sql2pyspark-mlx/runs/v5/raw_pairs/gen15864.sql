SELECT
    location,
    dept_name,
    MIN(budget) min_budget,
    MAX(budget) max_budget,
    COUNT(DISTINCT budget) unique_dept_names
FROM departments
GROUP BY 1, 2
ORDER BY min_budget
