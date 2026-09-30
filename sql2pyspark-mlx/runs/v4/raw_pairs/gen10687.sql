WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, ADD_MONTHS(hire_date, 3) AS hire_date_plus_months
    FROM employees
    WHERE level IS NOT NULL
),
enriched AS (
    -- Attach departments attributes
    SELECT src.*, p.dept_name
    FROM valid src
    LEFT JOIN departments p
        ON src.dept_id = p.id
),
joined AS (
    -- Attach employees attributes
    SELECT src.*, d.salary AS employee_salary
    FROM enriched src
    LEFT JOIN employees d
        ON src.manager_id = d.id
),
with_windows AS (
    -- Running and moving aggregates
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
        hire_date_plus_months,
        dept_name,
        employee_salary,
        NTILE(3) OVER (ORDER BY salary DESC, id) AS salary_bucket,
        NTILE(5) OVER w_recent AS salary_bucket_2
    FROM joined
    WINDOW w_recent AS (PARTITION BY manager_id ORDER BY salary, id)
),
metrics AS (
    -- Running and moving aggregates
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
        hire_date_plus_months,
        dept_name,
        employee_salary,
        salary_bucket,
        salary_bucket_2,
        LAST_VALUE(level, TRUE) OVER (PARTITION BY manager_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_level,
        MAX(salary) OVER (PARTITION BY manager_id) AS manager_id_max_salary,
        FIRST_VALUE(job_title) IGNORE NULLS OVER (ORDER BY salary_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_job_title
    FROM with_windows
),
metrics_employees AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY employee_salary DESC, id) AS row_seq,
        CUME_DIST() OVER (PARTITION BY job_title ORDER BY manager_id_max_salary DESC) AS manager_id_max_salary_cume_dist
    FROM metrics
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY job_title ORDER BY manager_id_max_salary NULLS FIRST) AS rank_in_group
        FROM metrics_employees
    ) ranked
    WHERE rank_in_group = 1
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            dept_id,
            dept_name,
            salary_bucket_2
        FROM deduped
    ) src
    PIVOT (
        MAX(salary_bucket_2) AS amount,
        COUNT(1) AS n
        FOR dept_name IN ('Marketing', 'Sales')
    )
)
SELECT dept_id, Marketing_n
FROM wide
ORDER BY Marketing_n DESC;
