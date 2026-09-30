WITH valid_employees AS (
    SELECT *
    FROM employees
    WHERE manager_id IS NOT NULL
),
level_group_summary AS (
    SELECT
        CASE WHEN level IN ('L5', 'L4') THEN 'core' ELSE 'other' END AS level_group,
        YEAR(hire_date) AS hire_year,
        SUM(CASE WHEN job_title = 'Manager' THEN 1 ELSE 0 END) AS manager_count,
        ROUND((100 * SUM(CASE WHEN job_title = 'Designer' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_designer
    FROM valid_employees
    GROUP BY CASE WHEN level IN ('L5', 'L4') THEN 'core' ELSE 'other' END, YEAR(hire_date)
)
SELECT
    level_group,
    hire_year,
    manager_count,
    pct_designer
FROM level_group_summary
WHERE manager_count >= 74
ORDER BY manager_count DESC, level_group, hire_year
LIMIT 10
