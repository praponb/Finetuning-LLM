WITH kept_keys AS (
    -- Set intersection on keys
    SELECT id, dept_name
    FROM departments
    INTERSECT ALL
    SELECT id, dept_name
    FROM departments
    WHERE location = 'New York'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM departments s
    JOIN kept_keys k ON s.id = k.id AND s.dept_name = k.dept_name
)
-- Aggregate with conditional metrics
SELECT
    dept_name,
    SUM(CASE WHEN dept_name = 'Finance' THEN budget ELSE 0 END) AS finance_budget,
    SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list
FROM verified
GROUP BY dept_name
ORDER BY dept_name DESC NULLS FIRST
