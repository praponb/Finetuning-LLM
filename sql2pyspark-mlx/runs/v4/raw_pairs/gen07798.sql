WITH base AS (
    SELECT
        *,
        CONCAT_WS('-', email, level) AS email_level_key,
        CASE WHEN is_remote THEN 'Yes' ELSE 'No' END AS is_remote_label
    FROM employees
    WHERE hire_date >= DATE '2023-05-26'
)
SELECT
    *,
    AVG(salary) OVER (PARTITION BY manager_id ORDER BY salary, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS salary_avg_5_prior,
    SUM(salary) OVER (ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_salary,
    AVG(salary) OVER (PARTITION BY job_title ORDER BY hire_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS salary_avg_2
FROM base
