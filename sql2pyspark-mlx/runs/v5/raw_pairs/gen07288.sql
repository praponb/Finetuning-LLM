WITH base AS (
    SELECT
        *,
        COALESCE(dept_name, 'unassigned') dept_name_filled,
        COALESCE(dept_name, 'UNKNOWN') dept_name_clean
    FROM departments
    WHERE location IS NOT NULL
),
not_flagged AS (
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.job_title = 'Intern'
        )
),
latest_match AS (
    SELECT s.*, top1.job_title
    FROM not_flagged s
    JOIN LATERAL (
            SELECT c.job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
cleaned AS (
    SELECT
        id,
        dept_name,
        location,
        budget,
        dept_name_clean,
        TRIM(dept_name) clean_dept_name
    FROM latest_match
    WHERE job_title IS NOT NULL
)
SELECT
    id,
    dept_name,
    location,
    budget,
    dept_name_clean
FROM cleaned
ORDER BY id DESC
