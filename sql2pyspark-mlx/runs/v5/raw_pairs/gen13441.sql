SELECT
    id,
    dept_name,
    location,
    budget,
    CONCAT_WS('/', location, dept_name) AS location_dept_name_key,
    CASE WHEN budget >= 4290000 THEN 'High' WHEN budget >= 682000 THEN 'Medium' ELSE 'Low' END AS budget_tier
FROM departments
WHERE dept_name IS NOT NULL;
