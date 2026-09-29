SELECT
    *,
    ROUND(salary / SUM(salary) OVER (PARTITION BY job_title, dept_id), 4) AS salary_share,
    SUM(salary) OVER (ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary
FROM employees;
