SELECT budget, CASE WHEN dept_name IN ('HR', 'Engineering') THEN 'Group A' ELSE 'Other' END AS dept_name_group, INITCAP(TRIM(dept_name)) AS dept_name_title
FROM departments
WHERE budget <= 1993000;
