SELECT
    id,
    dept_name,
    location,
    CASE WHEN budget >= 4135000 THEN 'High' WHEN budget >= 1317000 THEN 'Medium' ELSE 'Low' END AS budget_level
FROM departments
WHERE budget < 3642000 AND location = 'New York';
