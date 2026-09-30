WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        email,
        dept_id,
        manager_id,
        salary,
        hire_date,
        is_remote,
        level,
        LOWER(email) AS email_lower,
        COALESCE(job_title, 'N/A') AS resolved_job_title
    FROM employees
    WHERE hire_date >= DATE '2023-06-20'
      AND manager_id IS NOT NULL
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, DATE_SUB(hire_date, 21) AS hire_date_minus_21d
    FROM base
    WHERE level IS NOT NULL
      AND dept_id IN (8, 15, 20)
),
with_dims AS (
    -- Attach departments attributes
    SELECT a.*, d.budget
    FROM cleaned a
    LEFT JOIN departments d
        ON a.dept_id = d.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        email,
        dept_id,
        manager_id,
        salary,
        hire_date,
        is_remote,
        level,
        email_lower,
        resolved_job_title,
        hire_date_minus_21d,
        budget,
        SUM(budget) OVER (ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_budget,
        SUM(salary) OVER (PARTITION BY level ORDER BY hire_date_minus_21d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary
    FROM with_dims
),
joined AS (
    -- Attach departments attributes
    SELECT
        src.*,
        d.location,
        d.budget AS department_budget
    FROM scored src
    LEFT JOIN departments d
        ON src.dept_id = d.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN salary >= 148000 THEN 'HIGH' ELSE 'LOW' END AS salary_tier,
    PERCENTILE_APPROX(cumulative_budget, 0.9) AS cumulative_budget_p90,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT_IF(level = 'L5') AS l5_events
FROM joined
GROUP BY CASE WHEN salary >= 148000 THEN 'HIGH' ELSE 'LOW' END
ORDER BY distinct_ids DESC NULLS LAST;
