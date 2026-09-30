WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, LENGTH(dept_name) AS dept_name_length
    FROM departments
    WHERE dept_name IS NOT NULL
)
-- Deduplicate with ROW_NUMBER
SELECT *
FROM (
    SELECT *, RANK() OVER (PARTITION BY dept_name ORDER BY budget DESC) AS rank_in_group
    FROM valid
) t
WHERE rank_in_group = 1;
