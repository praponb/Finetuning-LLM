WITH cleaned AS (
    SELECT *, INITCAP(TRIM(dept_name)) AS dept_name_title
    FROM departments
    WHERE dept_name IS NOT NULL
)
SELECT s.*
FROM cleaned s
WHERE EXISTS (
        SELECT 1
        FROM employees x
        WHERE x.dept_id = s.id
          AND x.manager_id IS NOT NULL
    );
