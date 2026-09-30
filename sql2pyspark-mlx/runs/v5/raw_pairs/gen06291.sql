SELECT dept_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(job_title))) AS job_title_values, SUM(CASE WHEN job_title = 'Manager' THEN 1 ELSE 0 END) AS manager_count
FROM employees
WHERE hire_date BETWEEN '2023-10-14' AND '2025-09-17' AND salary > 57000
GROUP BY dept_id
HAVING manager_count >= 77
ORDER BY dept_id;
