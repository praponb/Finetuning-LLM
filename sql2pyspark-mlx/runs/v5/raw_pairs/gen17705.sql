SELECT job_title, SUM(salary) AS total_salary, SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list FROM employees WHERE dept_id = 22 GROUP BY job_title HAVING SUM(salary) < 726000
