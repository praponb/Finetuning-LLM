WITH filtered AS (
    SELECT
        *,
        CASE WHEN location IN ('Berlin', 'Bangkok') THEN 'Core' ELSE 'Other' END AS location_group,
        TRIM(dept_name) AS clean_dept_name
    FROM departments
    WHERE location IS NULL
),
not_flagged AS (
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.salary <= 144000
        )
),
kept AS (
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.dept_id IN (28, 30)
        )
),
latest_match AS (
    SELECT
        s.*,
        latest.employee_job_title,
        latest.employee_level
    FROM kept s
    LEFT JOIN LATERAL (
            SELECT c.job_title AS employee_job_title, c.level AS employee_level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC NULLS FIRST, id) AS rank_in_group
        FROM latest_match
    ) x
    WHERE rank_in_group = 1
)
SELECT
    CASE WHEN budget >= 4421000 THEN 'PREMIUM' ELSE 'STANDARD' END AS budget_tier,
    COUNT(*) AS event_count,
    PERCENTILE_APPROX(budget, 0.95) AS budget_p95
FROM latest
GROUP BY CASE WHEN budget >= 4421000 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING PERCENTILE_APPROX(budget, 0.95) >= 0
ORDER BY budget_tier DESC
