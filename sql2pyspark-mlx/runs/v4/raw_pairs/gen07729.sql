WITH with_dims AS (
    -- Attach departments attributes
    SELECT
        s.*,
        d.dept_name,
        d.location
    FROM employees s
    LEFT JOIN departments d
        ON s.dept_id = d.id
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', hire_date) AS activity_month,
    COUNT(DISTINCT dept_name) AS distinct_dept_names,
    APPROX_PERCENTILE(salary, ARRAY(0.25, 0.5, 0.75)) AS salary_quartiles,
    COUNT(CASE WHEN location = 'London' THEN 1 END) AS london_count,
    COUNT(*) AS num_rows,
    AVG(salary) AS avg_salary,
    COUNT(*) FILTER (WHERE dept_name = 'Legal') AS legal_rows
FROM with_dims
GROUP BY DATE_TRUNC('MONTH', hire_date);
