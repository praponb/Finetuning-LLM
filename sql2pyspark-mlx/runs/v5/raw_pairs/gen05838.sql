SELECT SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list, ROUND(AVG(salary), 2) AS avg_salary FROM employees WHERE hire_date <= '2024-01-01' ORDER BY avg_salary DESC
