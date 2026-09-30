WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, LOWER(dept_name) AS dept_name_lower
    FROM departments
    WHERE budget < 3153000
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY location, dept_name ORDER BY budget DESC NULLS FIRST) AS rank_in_group
    FROM cleaned
    QUALIFY rank_in_group = 1
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.email IS NULL
        )
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_name,
        budget,
        LEAD(budget, 1) OVER (PARTITION BY location ORDER BY budget, id) AS next_budget,
        CUME_DIST() OVER w_recent AS budget_cume_dist,
        ROUND(budget / SUM(budget) OVER (PARTITION BY location), 4) AS budget_share
    FROM kept
    WINDOW w_recent AS (PARTITION BY dept_name ORDER BY budget NULLS FIRST)
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.job_title,
        x.employee_hire_date
    FROM scored s
    JOIN LATERAL (
            SELECT c.job_title, c.hire_date AS employee_hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    dept_name,
    budget,
    next_budget,
    budget_cume_dist,
    job_title,
    employee_hire_date,
    CASE WHEN dept_name RLIKE '^(Sales|HR)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS dept_name_zone
FROM with_last;
