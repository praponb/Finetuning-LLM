WITH dim_totals AS (
    -- Join a dimension and aggregate
    SELECT
        p.location,
        p.dept_name,
        src.level,
        COUNT(*) AS rows_total,
        ROUND(SUM(src.salary), 2) AS total_salary
    FROM employees src
    INNER JOIN departments p ON src.dept_id = p.id
    GROUP BY ROLLUP (p.location, p.dept_name, src.level)
)
SELECT
    location,
    dept_name,
    level,
    rows_total,
    total_salary
FROM dim_totals;
