WITH base AS (
    SELECT
        *,
        LENGTH(location) AS location_length,
        COALESCE(dept_name, 'DIRECT') AS resolved_dept_name
    FROM departments
    WHERE location = 'Chiang Mai'
),
latest AS (
    SELECT *
    FROM base
    QUALIFY RANK() OVER (PARTITION BY dept_name ORDER BY location_length NULLS LAST) = 1
),
kept AS (
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.job_title = 'Director'
        )
),
kept_departments AS (
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.dept_id IN (2, 29, 37)
        )
),
not_flagged AS (
    SELECT s.*
    FROM kept_departments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.level NOT IN ('L5', 'L4')
        )
)
SELECT
    id,
    location,
    budget,
    resolved_dept_name,
    CASE WHEN location RLIKE '^(Berlin|New York)$' THEN 'GROUP_A' WHEN location RLIKE '^(Bangkok|Chiang Mai)$' THEN 'CORE' ELSE 'PRIORITY' END AS location_zone
FROM not_flagged
ORDER BY id DESC;
