WITH valid AS (
    SELECT
        *,
        SUBSTRING(dept_name, 1, 2) AS dept_name_prefix,
        COALESCE(location, 'NONE') AS location_clean
    FROM departments
    WHERE dept_name IN ('Legal', 'Sales')
)
SELECT id, budget
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC NULLS LAST, id) AS dedup_rank
    FROM valid
) ranked
WHERE dedup_rank = 1;
