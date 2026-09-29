WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TRIM(dept_name) AS clean_dept_name,
        CEIL(budget) AS budget_whole
    FROM departments
    WHERE location IS NOT NULL
)
-- Anti-join via a correlated subquery
SELECT s.*
FROM cleaned s
WHERE NOT EXISTS (
        SELECT 1
        FROM employees c
        WHERE c.dept_id = s.id
          AND c.level = 'L2'
    );
