SELECT COUNT(*) employee_count, SORT_ARRAY(COLLECT_SET(job_title)) job_title_list, ROUND(AVG(salary), 2) avg_salary
FROM employees
WHERE level IN ('L2', 'L5', 'L1') AND (dept_id IN (5, 16, 19) OR MONTH(hire_date) IN (2, 5, 7))
