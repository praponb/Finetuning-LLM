WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN level IN ('L4', 'L1') THEN 'Priority' ELSE 'Other' END AS level_group,
        COALESCE(level, 'DIRECT') AS level_clean
    FROM employees
    WHERE level IS NOT NULL
)
-- QUALIFY-style window filter
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY hire_date, id) AS row_num
    FROM cleaned
) ranked
WHERE row_num = 1;
