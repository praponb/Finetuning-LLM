SELECT s.*
FROM departments s
WHERE NOT EXISTS (
        SELECT 1
        FROM employees x
        WHERE x.dept_id = s.id
          AND x.dept_id = 15
    )
