SELECT
    dept_id,
    ROUND(AVG(salary), 2) AS avg_salary,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    MAX(salary) - MIN(salary) AS salary_range
FROM employees
GROUP BY 1
ORDER BY salary_range DESC, dept_id DESC
