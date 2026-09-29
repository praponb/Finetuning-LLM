WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM departments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND MONTH(c.hire_date) IN (8, 9, 12)
        )
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.employee_level,
        x.salary
    FROM kept s
    LEFT JOIN LATERAL (
            SELECT c.level AS employee_level, c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        location,
        budget,
        employee_level,
        salary,
        CUME_DIST() OVER (PARTITION BY employee_level ORDER BY budget DESC NULLS LAST) AS budget_cume_dist,
        SUM(salary) OVER (PARTITION BY dept_name ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary,
        LAST_VALUE(location) IGNORE NULLS OVER (PARTITION BY employee_level ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_location
    FROM latest_match
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, salary * budget AS salary_times_budget
    FROM scored
    WHERE employee_level IS NOT NULL
      AND running_salary BETWEEN 334395 AND 699360
)
SELECT
    id,
    location,
    employee_level,
    salary,
    budget_cume_dist,
    running_salary,
    last_known_location,
    CASE WHEN location RLIKE '^(New York|Bangkok)$' THEN 'CORE' WHEN location RLIKE '^(Chiang Mai|Singapore)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS location_zone
FROM base
ORDER BY salary DESC NULLS FIRST, budget_cume_dist
