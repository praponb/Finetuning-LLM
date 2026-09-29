WITH numbered AS (
    SELECT
        id,
        dept_id,
        salary,
        ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC, id) AS rn
    FROM employees
)
SELECT *
FROM numbered
WHERE rn <= 1;
