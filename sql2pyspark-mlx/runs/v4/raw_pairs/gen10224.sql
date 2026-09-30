WITH scored AS (
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        LAG(salary, 1) OVER (ORDER BY hire_date, id) AS prev_salary,
        FIRST_VALUE(level) IGNORE NULLS OVER (PARTITION BY dept_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_level,
        SUM(salary) OVER (ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_salary
    FROM employees
)
SELECT
    DATE_TRUNC('WEEK', hire_date) AS week_start,
    SUM(CASE WHEN job_title = 'Intern' THEN cumulative_salary ELSE 0 END) AS intern_cumulative_salary,
    SUM(CASE WHEN level = 'L2' THEN prev_salary ELSE 0 END) AS l2_prev_salary,
    COUNT(*) AS num_rows
FROM scored
GROUP BY DATE_TRUNC('WEEK', hire_date)
