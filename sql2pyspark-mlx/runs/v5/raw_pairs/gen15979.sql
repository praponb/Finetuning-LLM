SELECT
    DATE_TRUNC('MONTH', hire_date) AS activity_month,
    SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list,
    PERCENTILE_APPROX(salary, 0.5) AS salary_p50,
    ROUND(AVG(salary), 2) AS avg_salary
FROM employees
GROUP BY 1
HAVING PERCENTILE_APPROX(salary, 0.5) < 1000000
ORDER BY avg_salary
