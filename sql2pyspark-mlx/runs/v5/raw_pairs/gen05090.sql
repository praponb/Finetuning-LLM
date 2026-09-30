SELECT dept_name, unique_dept_names, singapore_budget
FROM (
    SELECT dept_name, COUNT(DISTINCT budget) AS unique_dept_names, SUM(CASE WHEN location = 'Singapore' THEN budget ELSE 0 END) AS singapore_budget
    FROM departments
    GROUP BY dept_name
) AS sub
WHERE singapore_budget >= 48
ORDER BY 1 DESC;
