WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, INITCAP(TRIM(dept_name)) AS dept_name_title
    FROM departments
    WHERE dept_name IS NOT NULL
      AND dept_name IN ('Sales', 'Engineering')
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.hire_date
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        dept_name,
        location,
        budget,
        hire_date,
        CAST(budget AS INT) AS budget_int
    FROM latest_match
    WHERE dept_name IS NOT NULL
)
-- Grouped metrics
SELECT
    location,
    DATE_TRUNC('YEAR', hire_date) AS year_start,
    APPROX_PERCENTILE(budget, 0.95) AS budget_p95,
    COUNT(*) FILTER (WHERE location = 'Bangkok') AS bangkok_rows,
    ROUND(SUM(budget) / NULLIF(COUNT(CASE WHEN dept_name = 'Sales' THEN 1 END), 0), 2) AS budget_per_sales,
    APPROX_PERCENTILE(budget, 0.95) AS budget_p95_2,
    SORT_ARRAY(COLLECT_SET(location)) AS location_list,
    AVG(budget_int) FILTER (WHERE location = 'Berlin') AS berlin_avg_budget_int
FROM cleaned
GROUP BY GROUPING SETS ((location, DATE_TRUNC('YEAR', hire_date)), (location));
