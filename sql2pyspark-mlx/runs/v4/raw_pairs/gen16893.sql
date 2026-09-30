WITH enriched AS (
    -- Attach departments attributes
    SELECT
        s.*,
        d.dept_name,
        d.budget,
        d.location
    FROM employees s
    LEFT JOIN departments d
        ON s.dept_id = d.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(salary) OVER (PARTITION BY level, dept_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary,
        NTILE(5) OVER (ORDER BY salary DESC, id) AS salary_bucket
    FROM enriched
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, budget * salary AS budget_times_salary
    FROM with_windows
    WHERE hire_date >= DATE '2024-02-23'
      AND location IS NOT NULL
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN running_salary >= 606531 THEN 'High' WHEN running_salary >= 120418 THEN 'Medium' ELSE 'Low' END AS running_salary_tier
    FROM valid
    WHERE hire_date >= DATE '2023-10-31'
      AND job_title IS NOT NULL
)
SELECT
    id,
    full_name,
    email,
    manager_id,
    job_title,
    salary,
    hire_date,
    is_remote,
    level
FROM cleaned
ORDER BY hire_date DESC NULLS LAST
