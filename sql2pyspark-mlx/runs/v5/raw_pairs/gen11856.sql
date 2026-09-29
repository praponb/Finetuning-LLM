WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(budget * 1.1, 2) AS budget_adjusted
    FROM departments
    WHERE location IS NOT NULL
      AND location IS NOT DISTINCT FROM 'Singapore'
),
filtered_departments AS (
    -- Keep recent, valid rows
    SELECT
        id,
        dept_name,
        location,
        budget,
        budget_adjusted,
        CAST(budget_adjusted AS INT) AS budget_adjusted_int,
        COALESCE(location, 'NONE') AS location_clean
    FROM filtered
    WHERE dept_name IS DISTINCT FROM 'Marketing'
      AND budget_adjusted < 71
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        dept_name,
        location,
        budget_adjusted,
        budget_adjusted_int,
        location_clean,
        INITCAP(TRIM(location)) AS location_title,
        budget_adjusted + budget_adjusted_int AS budget_adjusted_plus_budget_adjusted_int
    FROM filtered_departments
    WHERE budget <= 2955000
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.location,
        s.budget_adjusted,
        latest.job_title
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(job_title) IGNORE NULLS OVER (PARTITION BY job_title ORDER BY budget_adjusted ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_job_title,
        LAG(budget_adjusted, 2) OVER (PARTITION BY location ORDER BY budget_adjusted ASC, id ASC) AS prev_budget_adjusted,
        AVG(budget_adjusted) OVER (PARTITION BY job_title, location ORDER BY budget_adjusted ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS budget_adjusted_avg_2_prior,
        CUME_DIST() OVER (PARTITION BY job_title ORDER BY budget_adjusted ASC) AS budget_adjusted_cume_dist
    FROM latest_match
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND UPPER(x.full_name) LIKE '%IA %'
        )
)
SELECT
    id,
    location,
    budget_adjusted,
    job_title,
    last_known_job_title,
    budget_adjusted_avg_2_prior,
    budget_adjusted_cume_dist
FROM not_flagged
