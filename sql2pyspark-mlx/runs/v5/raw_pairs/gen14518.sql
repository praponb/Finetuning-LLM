WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(budget, 1) AS budget_rounded,
        COALESCE(dept_name, 'OTHER') AS dept_name_filled
    FROM departments
    WHERE location IS NOT NULL
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.hire_date,
        top1.employee_salary
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.hire_date, c.salary AS employee_salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM with_last
    EXCEPT ALL
    SELECT id
    FROM departments
    WHERE dept_name = 'Finance'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_last s
    INNER JOIN matched_keys k ON s.id = k.id
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.hire_date < s.hire_date
        )
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        location,
        dept_name,
        employee_salary
    FROM not_flagged
) src
PIVOT (
    SUM(employee_salary) AS amount
    FOR dept_name IN ('Marketing', 'Legal')
)
