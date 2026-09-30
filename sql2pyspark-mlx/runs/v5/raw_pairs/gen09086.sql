SELECT d.dept_name, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(e.job_title))) AS job_title_values
FROM employees e
INNER JOIN departments d ON e.dept_id = d.id
WHERE e.is_remote
GROUP BY d.dept_name
ORDER BY 2
