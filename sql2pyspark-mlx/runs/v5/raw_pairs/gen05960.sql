WITH filtered AS (
    SELECT
        *,
        INITCAP(TRIM(dept_name)) dept_name_title,
        CONCAT(location, ' - ', dept_name) location_dept_name
    FROM departments
    WHERE location IS NOT DISTINCT FROM 'Berlin'
      AND location IN ('Chiang Mai', 'Berlin')
)
SELECT s.*, x.level
FROM filtered s
LEFT JOIN LATERAL (
        SELECT c.level
        FROM employees c
        WHERE c.dept_id = s.id
        ORDER BY c.salary DESC, c.id DESC
        LIMIT 1
    ) x ON TRUE;
