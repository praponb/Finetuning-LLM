WITH numbered AS (
    SELECT
        id,
        dept_id,
        salary,
        is_remote,
        ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary, id) row_num
    FROM employees
    WHERE hire_date BETWEEN '2023-12-22' AND '2024-08-01'
)
SELECT
    id,
    dept_id,
    salary,
    is_remote
FROM numbered
WHERE row_num <= 3
ORDER BY dept_id DESC
