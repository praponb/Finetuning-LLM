SELECT s.*
FROM departments s
WHERE NOT EXISTS (
        SELECT 1
        FROM employees c
        WHERE c.dept_id = s.id
          AND YEAR(c.hire_date) = 2024
    )
