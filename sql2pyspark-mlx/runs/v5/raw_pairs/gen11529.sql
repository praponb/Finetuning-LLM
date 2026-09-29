WITH cleaned AS (
    SELECT *, CASE WHEN budget >= 3286000 THEN 'High' WHEN budget >= 1452000 THEN 'Medium' ELSE 'Low' END AS budget_level
    FROM departments
    WHERE location IS NOT NULL
      AND budget < 1512000
),
with_last AS (
    SELECT
        s.*,
        latest.employee_job_title,
        latest.hire_date
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.job_title AS employee_job_title, c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept AS (
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.hire_date >= s.hire_date
        )
),
kept_departments AS (
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND LOWER(TRIM(ch.email)) = 'henry@startup.io'
        )
),
remaining_keys AS (
    SELECT dept_name, location
    FROM kept_departments
    EXCEPT
    SELECT dept_name, location
    FROM departments
    WHERE dept_name = 'Sales'
),
kept_rows AS (
    SELECT s.*
    FROM kept_departments s
    JOIN remaining_keys k ON s.dept_name = k.dept_name AND s.location = k.location
),
summary AS (
    SELECT
        location,
        STDDEV_SAMP(budget) AS budget_stddev,
        COUNT(DISTINCT location) AS distinct_locations,
        COUNT(*) AS event_count,
        SIZE(COLLECT_SET(dept_name)) AS num_dept_names
    FROM kept_rows
    GROUP BY location
    HAVING COUNT(*) > 1
)
SELECT
    location,
    budget_stddev,
    distinct_locations
FROM summary
ORDER BY location NULLS FIRST, budget_stddev DESC
