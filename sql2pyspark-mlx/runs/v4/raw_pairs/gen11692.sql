SELECT COUNT(DISTINCT budget) AS unique_dept_names, MIN(budget) AS min_budget
FROM departments
WHERE budget BETWEEN 1592000 AND 3377000
