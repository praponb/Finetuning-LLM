WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, INITCAP(TRIM(location)) AS location_title
    FROM departments
    WHERE dept_name IS NOT NULL
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND YEAR(c.hire_date) >= 2025
        )
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.salary,
        x.level
    FROM kept s
    JOIN LATERAL (
            SELECT c.salary, c.level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Multi-level aggregation
SELECT
    level,
    CASE WHEN salary >= 152000 THEN 'PREMIUM' ELSE 'STANDARD' END AS salary_tier,
    SUM(budget) FILTER (WHERE location = 'Chiang Mai') AS chiang_mai_total_budget,
    COUNT_IF(dept_name = 'Finance') AS finance_events,
    RANK() OVER (PARTITION BY level ORDER BY COUNT_IF(dept_name = 'Finance') DESC) AS finance_events_rank
FROM with_latest
GROUP BY level, salary_tier
ORDER BY finance_events DESC NULLS FIRST;
