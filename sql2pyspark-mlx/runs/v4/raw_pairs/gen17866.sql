WITH metrics AS (
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
        SUM(salary) OVER (PARTITION BY job_title ORDER BY hire_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_salary,
        ROUND(salary / SUM(salary) OVER (PARTITION BY dept_id, job_title), 4) AS salary_share
    FROM employees
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY hire_date ASC, id ASC) = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(salary_share, 1) OVER (PARTITION BY dept_id ORDER BY cumulative_salary ASC, id ASC) AS prev_salary_share,
        AVG(cumulative_salary) OVER w_ordered AS cumulative_salary_avg_5,
        FIRST_VALUE(job_title, TRUE) OVER (ORDER BY salary ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_job_title
    FROM deduped
    WINDOW w_ordered AS (PARTITION BY level ORDER BY hire_date ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
),
enriched_employees AS (
    -- Attach departments attributes
    SELECT
        src.*,
        b.location,
        b.dept_name
    FROM enriched src
    INNER JOIN departments b
        ON src.dept_id = b.id
),
enriched_employees_2 AS (
    -- Attach employees attributes
    SELECT a.*, p.level AS employee_level
    FROM enriched_employees a
    LEFT JOIN employees p
        ON a.manager_id = p.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        dept_id,
        employee_level,
        job_title,
        salary_share
    FROM enriched_employees_2
) src
PIVOT (
    AVG(salary_share) AS value_sum
    FOR job_title IN ('Analyst' AS analyst, 'Manager' AS manager)
)
