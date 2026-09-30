WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN dept_name IN ('Legal', 'Sales') THEN 'Tier 1' ELSE 'Other' END AS dept_name_group,
        COALESCE(dept_name, 'OTHER') AS resolved_dept_name
    FROM departments
    WHERE dept_name IS NOT NULL
      AND location IS DISTINCT FROM 'Berlin'
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.budget,
        s.dept_name_group,
        top1.level
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.budget,
        s.dept_name_group,
        s.level,
        x.hire_date,
        x.salary
    FROM with_latest s
    JOIN LATERAL (
            SELECT c.hire_date, c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.hire_date <= s.hire_date
        )
),
totals AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('YEAR', hire_date) AS year_start,
        DATE_TRUNC('WEEK', hire_date) AS week_start,
        CASE WHEN budget >= 3329000 THEN 'HIGH' ELSE 'LOW' END AS budget_tier,
        AVG(salary) FILTER (WHERE level = 'L5') AS l5_avg_salary,
        SIZE(COLLECT_SET(level)) AS num_levels,
        RANK() OVER (PARTITION BY DATE_TRUNC('YEAR', hire_date) ORDER BY SIZE(COLLECT_SET(level)) DESC) AS num_levels_rank
    FROM not_flagged
    GROUP BY DATE_TRUNC('YEAR', hire_date), DATE_TRUNC('WEEK', hire_date), CASE WHEN budget >= 3329000 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    year_start,
    week_start,
    budget_tier,
    l5_avg_salary
FROM totals
ORDER BY budget_tier DESC NULLS LAST;
