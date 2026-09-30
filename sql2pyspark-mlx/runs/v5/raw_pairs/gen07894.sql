WITH valid AS (
    SELECT
        *,
        LENGTH(dept_name) AS dept_name_length,
        CASE WHEN dept_name IN ('Marketing', 'Legal') THEN 'Group A' ELSE 'Other' END AS dept_name_group
    FROM departments
    WHERE location IS NOT NULL
)
SELECT id, location
FROM valid
EXCEPT DISTINCT
SELECT id, location
FROM departments
WHERE dept_name = 'Sales';
