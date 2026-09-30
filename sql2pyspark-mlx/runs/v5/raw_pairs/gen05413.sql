WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(location) IGNORE NULLS OVER w_part AS last_known_location,
        FIRST_VALUE(dept_name, TRUE) OVER w_recent AS first_known_dept_name,
        NTILE(4) OVER (PARTITION BY location, dept_name ORDER BY budget, id) AS budget_bucket
    FROM departments
    WINDOW w_part AS (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.employee_hire_date
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.hire_date AS employee_hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY employee_hire_date DESC NULLS FIRST, id) AS rank_in_group
        FROM latest_match
    ) x
    WHERE rank_in_group = 1
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.dept_id = 9
        )
)
SELECT
    id,
    dept_name,
    budget,
    first_known_dept_name,
    budget_bucket
FROM kept
ORDER BY id, dept_name DESC
LIMIT 10
