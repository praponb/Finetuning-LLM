WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(job_title) IGNORE NULLS OVER w_recent AS first_known_job_title,
        ROUND(salary / SUM(salary) OVER (PARTITION BY level), 4) AS salary_share,
        MAX(salary) OVER (PARTITION BY level, manager_id) AS level_max_salary
    FROM employees
    WINDOW w_recent AS (PARTITION BY level ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched_employees AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(salary) OVER (PARTITION BY level, manager_id) AS level_sum_salary,
        ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY level_max_salary, id) AS event_seq,
        LEAD(salary_share, 1, 0) OVER (PARTITION BY manager_id ORDER BY salary, id) AS next_salary_share
    FROM enriched
    WHERE salary_share <= 0.59
),
joined AS (
    -- Attach departments attributes
    SELECT
        a.id,
        a.full_name,
        a.dept_id,
        a.manager_id,
        a.job_title,
        a.salary,
        a.hire_date,
        a.first_known_job_title,
        a.salary_share,
        a.level_max_salary,
        a.event_seq,
        a.next_salary_share,
        b.dept_name,
        b.budget,
        b.location
    FROM enriched_employees a
    LEFT JOIN departments b
        ON a.dept_id = b.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        hire_date,
        first_known_job_title,
        salary_share,
        level_max_salary,
        event_seq,
        next_salary_share,
        dept_name,
        location,
        ROUND(level_max_salary * 1.15, 2) AS adjusted_level_max_salary,
        GREATEST(salary_share, salary) AS greatest_salary_share_salary
    FROM joined
    WHERE location IS NOT NULL
      AND next_salary_share > 0.36
)
SELECT
    id,
    full_name,
    dept_id,
    manager_id,
    first_known_job_title,
    level_max_salary,
    event_seq,
    next_salary_share,
    dept_name
FROM base;
