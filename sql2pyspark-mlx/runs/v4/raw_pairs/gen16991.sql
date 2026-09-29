WITH valid AS (
    SELECT
        id,
        full_name,
        email,
        manager_id,
        salary,
        hire_date,
        SUBSTRING(email, 1, 3) AS email_prefix
    FROM employees
    WHERE hire_date > DATE '2023-10-09'
      AND manager_id IS NOT NULL
      AND salary >= 108000
),
deduped AS (
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY hire_date DESC, id) = 1
),
joined AS (
    SELECT
        src.*,
        d.level,
        d.job_title,
        d.email AS employee_email
    FROM deduped src
    LEFT JOIN employees d
        ON src.manager_id = d.id
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary DESC, id) AS latest_rank
    FROM joined
    QUALIFY latest_rank = 1
),
scored AS (
    SELECT
        *,
        LEAD(salary, 1) OVER (PARTITION BY level ORDER BY latest_rank, id) AS next_salary,
        LAST_VALUE(level) IGNORE NULLS OVER (PARTITION BY job_title, manager_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_level,
        NTILE(5) OVER (PARTITION BY level, manager_id ORDER BY salary, id) AS salary_bucket,
        LAST_VALUE(level) IGNORE NULLS OVER (PARTITION BY job_title ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_level_2,
        ROUND(salary / SUM(salary) OVER (PARTITION BY manager_id), 4) AS salary_share
    FROM ranked
),
scored_employees AS (
    SELECT
        id,
        full_name,
        email,
        manager_id,
        salary,
        hire_date,
        email_prefix,
        level,
        job_title,
        employee_email,
        latest_rank,
        next_salary,
        last_known_level,
        salary_bucket,
        last_known_level_2,
        salary_share,
        SUM(salary) OVER (PARTITION BY level ORDER BY salary_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary,
        PERCENT_RANK() OVER (PARTITION BY job_title, level ORDER BY salary DESC) AS salary_percent_rank
    FROM scored
)
SELECT
    id,
    full_name,
    email,
    manager_id,
    salary,
    hire_date,
    email_prefix,
    employee_email,
    latest_rank
FROM scored_employees
ORDER BY id NULLS LAST, salary
LIMIT 5;
