WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        INITCAP(TRIM(email)) AS email_title,
        COALESCE(level, 'OTHER') AS resolved_level
    FROM employees
    WHERE manager_id IS NOT NULL
),
joined AS (
    -- Attach employees attributes
    SELECT
        a.*,
        b.full_name AS employee_full_name,
        b.level
    FROM filtered a
    LEFT JOIN employees b
        ON a.manager_id = b.id
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        dept_id,
        hire_date,
        email_title,
        resolved_level,
        level,
        CONCAT(email, ' ', employee_full_name) AS email_employee_full_name,
        COALESCE(job_title, 'DIRECT') AS job_title_filled
    FROM joined
    WHERE level IS NOT NULL
      AND level IS DISTINCT FROM 'L1'
),
enriched AS (
    -- Attach departments attributes
    SELECT
        e.id,
        e.full_name,
        e.dept_id,
        e.hire_date,
        e.resolved_level,
        e.level,
        e.email_employee_full_name,
        e.job_title_filled,
        d.budget,
        d.location,
        d.dept_name
    FROM base e
    LEFT JOIN departments d
        ON e.dept_id = d.id
),
filtered_employees AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        dept_id,
        hire_date,
        resolved_level,
        level,
        email_employee_full_name,
        job_title_filled,
        budget,
        location,
        dept_name,
        CONCAT_WS('-', level, full_name) AS level_full_name_key,
        COALESCE(level, 'N/A') AS resolved_level_2
    FROM enriched
    WHERE hire_date > DATE '2023-04-25'
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', hire_date) AS activity_month,
    DATE_TRUNC('QUARTER', hire_date) AS quarter_start,
    COUNT(CASE WHEN level = 'L3' THEN 1 END) AS l3_count,
    SUM(CASE WHEN location = 'Bangkok' THEN budget ELSE 0 END) AS bangkok_budget,
    PERCENTILE_APPROX(budget, ARRAY(0.25, 0.5, 0.75)) AS budget_quartiles
FROM filtered_employees
GROUP BY ROLLUP (DATE_TRUNC('MONTH', hire_date), DATE_TRUNC('QUARTER', hire_date))
ORDER BY l3_count DESC NULLS LAST
