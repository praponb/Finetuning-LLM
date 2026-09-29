WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(is_remote AS INT) AS is_remote_flag
    FROM employees
    WHERE hire_date > TO_DATE('2023-06-01')
      AND job_title IS NOT NULL
)
-- Join a dimension and aggregate
SELECT
    d.location,
    src.level,
    COUNT(*) AS rows_total,
    AVG(src.salary) AS avg_salary
FROM cleaned src
JOIN departments d ON src.dept_id = d.id
GROUP BY GROUPING SETS ((d.location, src.level), (d.location))
ORDER BY avg_salary DESC NULLS FIRST
