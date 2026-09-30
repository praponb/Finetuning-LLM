WITH kept AS (
    SELECT s.*
    FROM departments s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.manager_id IS NOT NULL
        )
),
base AS (
    SELECT
        *,
        COALESCE(dept_name, 'Unknown') AS dept_name_filled,
        CEIL(budget) AS budget_whole,
        COALESCE(dept_name, 'N/A') AS resolved_dept_name
    FROM kept
    WHERE dept_name IS NOT NULL
      AND budget <= 1896000
),
kept_departments AS (
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND UPPER(ch.email) LIKE '%MMA%'
        )
),
deduped AS (
    SELECT *
    FROM kept_departments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY budget, id) = 1
)
SELECT
    location,
    APPROX_PERCENTILE(budget, 0.5) AS budget_p50,
    SUM(CASE WHEN dept_name = 'HR' THEN budget ELSE 0 END) AS hr_budget,
    ROUND(AVG(budget_whole), 2) AS avg_budget_whole
FROM deduped
GROUP BY location
HAVING ROUND(AVG(budget_whole), 2) >= 0
ORDER BY hr_budget NULLS LAST;
