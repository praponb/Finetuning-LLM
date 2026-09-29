WITH enriched AS (
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
        DENSE_RANK() OVER (PARTITION BY job_title ORDER BY salary DESC) AS salary_dense_rank,
        AVG(salary) OVER (PARTITION BY job_title ORDER BY hire_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS salary_avg_5
    FROM employees
),
latest AS (
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary_avg_5 NULLS LAST, id) = 1
),
with_dims AS (
    SELECT e.*, p.full_name AS employee_full_name
    FROM latest e
    LEFT JOIN employees p
        ON e.manager_id = p.id
),
metrics AS (
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
        salary_dense_rank,
        salary_avg_5,
        employee_full_name,
        ROUND(salary_avg_5 / SUM(salary_avg_5) OVER (PARTITION BY manager_id, dept_id), 4) AS salary_avg_5_share,
        NTILE(10) OVER (PARTITION BY job_title ORDER BY salary, id) AS salary_bucket,
        ROW_NUMBER() OVER (ORDER BY salary_dense_rank, id) AS row_seq
    FROM with_dims
),
joined AS (
    SELECT
        s.*,
        d.dept_name,
        d.budget,
        d.location
    FROM metrics s
    LEFT JOIN departments d
        ON s.dept_id = d.id
),
metrics_employees AS (
    SELECT
        *,
        FIRST_VALUE(salary_bucket, TRUE) OVER (PARTITION BY manager_id ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_salary_bucket,
        NTILE(3) OVER (PARTITION BY location, job_title ORDER BY salary DESC, id) AS salary_bucket_2,
        FIRST_VALUE(salary_avg_5_share, TRUE) OVER (PARTITION BY location ORDER BY salary_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_salary_avg_5_share
    FROM joined
)
SELECT *
FROM (
    SELECT
        manager_id,
        location,
        job_title,
        salary_avg_5_share
    FROM metrics_employees
) src
PIVOT (
    SUM(salary_avg_5_share) AS value_sum,
    COUNT(1) AS item_count
    FOR job_title IN ('Intern' AS intern, 'Designer' AS designer, 'Director' AS director)
)
