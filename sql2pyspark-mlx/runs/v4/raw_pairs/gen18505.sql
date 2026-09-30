SELECT dept_name, budget, CASE WHEN dept_name IN ('Marketing', 'HR') THEN 'Tier 1' ELSE 'Other' END AS dept_name_group
FROM departments
