SELECT
    *,
    AVG(salary) OVER (ORDER BY hire_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS salary_avg_5_prior,
    LAG(salary, 2) OVER (PARTITION BY level, dept_id ORDER BY salary, id) AS prev_salary,
    AVG(salary) OVER (PARTITION BY level ORDER BY hire_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS salary_avg_3_prior
FROM employees;
