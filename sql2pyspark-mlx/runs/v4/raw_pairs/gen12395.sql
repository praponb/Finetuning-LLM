WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, TRIM(dept_name) clean_dept_name
    FROM departments
    WHERE dept_name NOT IN ('Sales', 'Legal')
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY budget NULLS LAST, id) AS rank_in_group
        FROM valid
    ) x
    WHERE rank_in_group = 1
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.level
    FROM ranked s
    JOIN LATERAL (
            SELECT c.level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    dept_name,
    location,
    budget,
    clean_dept_name,
    rank_in_group,
    level
FROM with_latest
ORDER BY rank_in_group DESC NULLS FIRST, level
