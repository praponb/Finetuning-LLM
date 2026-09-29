WITH kept_keys AS (
    -- Set difference on keys
    SELECT id, dept_name
    FROM departments
    EXCEPT ALL
    SELECT id, dept_name
    FROM departments
    WHERE budget BETWEEN 1088000 AND 1329000
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM departments s
    JOIN kept_keys k ON s.id = k.id AND s.dept_name = k.dept_name
)
-- Grouped metrics
SELECT
    location,
    STDDEV_SAMP(budget) AS budget_stddev,
    ROUND(AVG(budget), 2) AS avg_budget,
    SIZE(COLLECT_SET(dept_name)) AS num_dept_names
FROM verified
GROUP BY location;
