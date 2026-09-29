SELECT SUM(salary) total_salary, SORT_ARRAY(COLLECT_SET(level)) level_list, SUM(CASE WHEN job_title = 'Director' THEN 1 ELSE 0 END) director_count
FROM employees
ORDER BY total_salary
