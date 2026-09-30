SELECT
    dept_name,
    CASE WHEN dept_name IN ('Legal', 'Engineering') THEN 'core' ELSE 'other' END AS dept_name_group,
    SUM(budget) AS total_budget,
    ROUND(STDDEV(budget), 2) AS stddev_budget
FROM departments
GROUP BY CUBE(dept_name, CASE WHEN dept_name IN ('Legal', 'Engineering') THEN 'core' ELSE 'other' END)
ORDER BY dept_name DESC
