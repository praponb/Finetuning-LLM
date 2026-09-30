SELECT id, dept_name, CASE WHEN dept_name IN ('Legal', 'Engineering') THEN 'Tier 1' ELSE 'Other' END AS dept_name_group
FROM departments
WHERE location NOT IN ('London', 'Berlin', 'New York')
