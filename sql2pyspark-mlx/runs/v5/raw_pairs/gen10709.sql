SELECT d.location, CASE WHEN d.dept_name IN ('Engineering', 'Legal') THEN 'priority' ELSE 'other' END AS dept_name_group, SORT_ARRAY(COLLECT_SET(e.job_title)) AS job_title_list
FROM employees e
RIGHT JOIN departments d ON e.dept_id = d.id
WHERE e.dept_id = 10
GROUP BY d.location, CASE WHEN d.dept_name IN ('Engineering', 'Legal') THEN 'priority' ELSE 'other' END
ORDER BY location DESC, dept_name_group
