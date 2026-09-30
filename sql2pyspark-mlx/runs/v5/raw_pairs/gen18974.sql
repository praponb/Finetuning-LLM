WITH top_departments AS (
    SELECT *, RANK() OVER (PARTITION BY dept_name ORDER BY budget) rn
    FROM departments
)
SELECT id, dept_name, budget
FROM top_departments
WHERE rn = 1
