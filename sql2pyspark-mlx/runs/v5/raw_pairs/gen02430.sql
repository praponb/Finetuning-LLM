WITH cleaned AS (
    SELECT
        *,
        CEIL(budget) AS budget_whole,
        COALESCE(location, 'NONE') AS location_filled
    FROM departments
    WHERE location IS NOT NULL
      AND dept_name <> 'HR'
),
scored AS (
    SELECT
        id,
        dept_name,
        NTILE(10) OVER (PARTITION BY dept_name ORDER BY budget_whole, id) AS budget_whole_bucket,
        ROUND(budget / SUM(budget) OVER (PARTITION BY location), 4) AS budget_share,
        ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY budget, id) AS event_seq
    FROM cleaned
),
eligible AS (
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.job_title NOT IN ('Manager', 'Intern')
        )
),
kept AS (
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.dept_id = 30
        )
)
SELECT
    id,
    budget_whole_bucket,
    budget_share,
    event_seq
FROM kept
ORDER BY event_seq, budget_whole_bucket;
