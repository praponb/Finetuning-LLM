WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(salary AS INT) AS salary_int,
        COALESCE(job_title, 'NONE') AS job_title_clean
    FROM employees
    WHERE dept_id IN (1, 9)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(salary_int, 1) OVER (ORDER BY salary, id) AS next_salary_int,
        SUM(salary_int) OVER (PARTITION BY manager_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary_int
    FROM base
),
joined AS (
    -- Attach departments attributes
    SELECT a.*, d.budget
    FROM enriched a
    LEFT JOIN departments d
        ON a.dept_id = d.id
),
scored AS (
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
        salary_int,
        job_title_clean,
        next_salary_int,
        running_salary_int,
        budget,
        LAST_VALUE(running_salary_int, TRUE) OVER (PARTITION BY manager_id ORDER BY next_salary_int, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_running_salary_int,
        FIRST_VALUE(salary_int) IGNORE NULLS OVER (PARTITION BY dept_id ORDER BY salary_int, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_salary_int
    FROM joined
)
-- Multi-level aggregation
SELECT
    CASE WHEN budget >= 4369000 THEN 'PREMIUM' ELSE 'STANDARD' END AS budget_tier,
    ROUND(AVG(salary_int), 2) AS avg_salary_int,
    ROUND(AVG(running_salary_int), 2) AS avg_running_salary_int,
    COUNT(*) FILTER (WHERE level = 'L3') AS l3_rows
FROM scored
GROUP BY budget_tier;
