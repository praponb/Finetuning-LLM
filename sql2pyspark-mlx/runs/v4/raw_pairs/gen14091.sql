WITH filtered_employees AS (
    SELECT *
    FROM employees
    WHERE manager_id IS NULL
),
summary AS (
    SELECT level, MAX(salary) - MIN(salary) AS salary_range, MAX(salary) AS max_salary
    FROM filtered_employees
    GROUP BY level
)
SELECT level, max_salary
FROM summary
WHERE salary_range < 53
ORDER BY max_salary;
