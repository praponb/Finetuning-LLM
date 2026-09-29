WITH base AS (
    SELECT
        id,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        DATE_SUB(hire_date, 51) AS hire_date_minus_51d
    FROM employees
    WHERE hire_date > DATE '2023-06-28'
      AND level IS NOT NULL
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id, dept_id ORDER BY hire_date_minus_51d DESC, id) AS latest_rank
    FROM base
    QUALIFY latest_rank = 1
),
metrics AS (
    SELECT
        *,
        LAG(salary, 2) OVER (PARTITION BY manager_id ORDER BY latest_rank, id) AS prev_salary,
        NTILE(3) OVER (PARTITION BY manager_id ORDER BY salary, id) AS salary_bucket,
        ROUND(salary / SUM(salary) OVER (PARTITION BY manager_id), 4) AS salary_share
    FROM ranked
),
with_windows AS (
    SELECT
        *,
        NTILE(10) OVER w_ordered AS salary_bucket_bucket,
        COUNT(prev_salary) OVER (PARTITION BY job_title) AS job_title_count_prev_salary,
        AVG(prev_salary) OVER w_recent AS prev_salary_avg_6,
        ROW_NUMBER() OVER (ORDER BY hire_date_minus_51d, id) AS event_seq
    FROM metrics
    WHERE is_remote
    WINDOW w_ordered AS (PARTITION BY job_title ORDER BY salary_bucket, id), w_recent AS (PARTITION BY dept_id, job_title ORDER BY prev_salary, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
enriched AS (
    SELECT a.*, p.dept_name
    FROM with_windows a
    JOIN departments p
        ON a.dept_id = p.id
)
SELECT
    s.id,
    s.email,
    s.manager_id,
    s.salary,
    s.hire_date,
    s.level,
    s.hire_date_minus_51d,
    s.latest_rank,
    s.salary_bucket,
    (SELECT MAX(t2.salary) FROM employees t2 WHERE t2.manager_id = s.manager_id) AS max_employee_salary
FROM enriched s
ORDER BY level DESC
