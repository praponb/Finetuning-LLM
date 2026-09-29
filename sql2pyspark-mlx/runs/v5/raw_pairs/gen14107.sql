WITH cleaned AS (
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
        SUBSTRING(email, 1, 4) AS email_prefix
    FROM employees
    WHERE job_title IS DISTINCT FROM 'Engineer'
      AND salary <= 96000
),
with_windows AS (
    SELECT
        *,
        FIRST_VALUE(level) IGNORE NULLS OVER w_grp AS first_known_level,
        AVG(salary) OVER (PARTITION BY manager_id ORDER BY salary, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS salary_avg_5_prior,
        LEAD(salary, 2) OVER (PARTITION BY job_title, manager_id ORDER BY hire_date, id) AS next_salary,
        NTILE(4) OVER w_part AS salary_bucket
    FROM cleaned
    WINDOW w_grp AS (ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY level ORDER BY salary DESC, id)
),
base AS (
    SELECT
        id,
        email,
        manager_id,
        job_title,
        salary,
        hire_date,
        level,
        email_prefix,
        first_known_level,
        salary_avg_5_prior,
        next_salary,
        salary_bucket,
        CAST(is_remote AS INT) AS is_remote_flag,
        ROUND(next_salary, 0) AS next_salary_rounded
    FROM with_windows
    WHERE level IS NOT NULL
      AND job_title IS NOT DISTINCT FROM 'Manager'
),
ranked AS (
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY hire_date DESC, id) = 1
)
SELECT
    e.level AS employee_level,
    e.job_title AS employee_job_title,
    COUNT(*) AS rows_total,
    SUM(s.salary_bucket) AS total_salary_bucket
FROM ranked s
JOIN employees e ON s.manager_id = e.id
GROUP BY e.level, e.job_title
ORDER BY employee_level DESC NULLS LAST
