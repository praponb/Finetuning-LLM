SELECT
    DATE_FORMAT(hire_date, 'yyyy-MM') AS hire_month,
    MAX(hire_date) AS last_hire_date,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(job_title))) AS job_title_values,
    ROUND(AVG(salary), 2) AS avg_salary
FROM employees
GROUP BY 1
HAVING avg_salary < 30
ORDER BY job_title_values DESC, avg_salary DESC
