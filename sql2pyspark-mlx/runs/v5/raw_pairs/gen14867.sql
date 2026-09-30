WITH base AS (
    SELECT
        *,
        CONCAT(location, '/', dept_name) location_dept_name,
        COALESCE(dept_name, 'NONE') dept_name_filled
    FROM departments
    WHERE location IN ('Bangkok', 'Chiang Mai', 'Singapore')
),
not_flagged AS (
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.level NOT IN ('L1', 'L4')
        )
),
with_windows AS (
    SELECT
        *,
        LAG(budget, 1) OVER (PARTITION BY dept_name ORDER BY budget, id) prev_budget,
        LAST_VALUE(budget) IGNORE NULLS OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_budget
    FROM not_flagged
),
with_last AS (
    SELECT
        s.*,
        top1.level,
        top1.hire_date
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.level, c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_latest AS (
    SELECT
        s.id,
        s.location,
        s.budget,
        s.dept_name_filled,
        s.hire_date,
        latest.employee_job_title,
        latest.employee_hire_date
    FROM with_last s
    JOIN LATERAL (
            SELECT c.job_title AS employee_job_title, c.hire_date AS employee_hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
totals AS (
    SELECT
        location,
        ROUND(SUM(budget) / NULLIF(COUNT(CASE WHEN location = 'London' THEN 1 END), 0), 2) AS budget_per_london,
        APPROX_PERCENTILE(budget, ARRAY(0.25, 0.5, 0.75)) AS budget_quartiles,
        COUNT_IF(employee_job_title = 'Engineer') AS engineer_events,
        PERCENTILE_APPROX(budget, 0.9) AS budget_p90,
        GROUPING(location) AS gid
    FROM with_latest
    GROUP BY CUBE (location)
)
SELECT budget_per_london, budget_p90
FROM totals
ORDER BY budget_p90 NULLS LAST
