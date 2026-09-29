WITH filtered AS (
    SELECT
        id,
        full_name,
        email,
        dept_id,
        salary,
        hire_date,
        is_remote,
        level,
        COALESCE(job_title, 'Unknown') AS job_title_filled,
        CAST(is_remote AS INT) AS is_remote_flag
    FROM employees
    WHERE job_title IS NOT NULL
      AND hire_date >= '2025-05-30'
),
enriched AS (
    SELECT
        e.*,
        p.location,
        p.dept_name,
        p.budget
    FROM filtered e
    LEFT JOIN departments p
        ON e.dept_id = p.id
    WHERE p.budget BETWEEN 1277000 AND 2861000
),
joined AS (
    SELECT
        s.id,
        s.full_name,
        s.dept_id,
        s.salary,
        s.is_remote,
        s.job_title_filled,
        s.location,
        s.dept_name,
        s.budget,
        b.budget AS department_budget,
        b.dept_name AS department_dept_name,
        b.location AS department_location
    FROM enriched s
    LEFT JOIN departments b
        ON s.dept_id = b.id
),
base AS (
    SELECT
        *,
        INITCAP(TRIM(location)) AS location_title,
        ROUND(budget * 1.1, 2) AS budget_scaled,
        COALESCE(dept_name, 'UNKNOWN') AS resolved_dept_name
    FROM joined
    WHERE location IS NOT NULL
      AND dept_name IS DISTINCT FROM 'Engineering'
),
valid AS (
    SELECT *, GREATEST(department_budget, salary) AS greatest_department_budget_salary
    FROM base
    WHERE department_location IS NOT NULL
),
daily AS (
    SELECT
        dept_id,
        CASE WHEN department_budget >= 4020000 THEN 'LARGE' ELSE 'SMALL' END AS department_budget_tier,
        location,
        COUNT(*) FILTER (WHERE department_dept_name = 'Sales') AS sales_rows,
        APPROX_PERCENTILE(salary, 0.5) AS salary_p50,
        COUNT(DISTINCT dept_name) AS distinct_dept_names
    FROM valid
    GROUP BY dept_id, CASE WHEN department_budget >= 4020000 THEN 'LARGE' ELSE 'SMALL' END, location
    HAVING salary_p50 > 0
)
SELECT
    dept_id,
    department_budget_tier,
    location,
    salary_p50,
    distinct_dept_names,
    CASE WHEN location RLIKE '^(London|Bangkok)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS location_zone
FROM daily
ORDER BY dept_id DESC
