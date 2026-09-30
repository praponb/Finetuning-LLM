WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(budget, 0) AS budget_filled,
        INITCAP(TRIM(dept_name)) AS dept_name_title,
        COALESCE(dept_name, 'N/A') AS resolved_dept_name
    FROM departments
    WHERE location = 'Chiang Mai'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        dept_name,
        location,
        budget,
        budget_filled,
        dept_name_title
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY budget NULLS LAST, id) AS dedup_rank
        FROM valid
    ) x
    WHERE dedup_rank = 1
),
deduped_departments AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget_filled DESC, id) AS row_num
        FROM deduped
    ) ranked
    WHERE row_num = 1
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped_departments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.dept_id IN (5, 8, 37, 40)
        )
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.job_title = 'Manager'
        )
)
SELECT
    id,
    budget_filled,
    dept_name_title
FROM eligible
ORDER BY budget_filled DESC NULLS FIRST
