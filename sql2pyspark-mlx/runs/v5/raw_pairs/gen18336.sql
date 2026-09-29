WITH numbered AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY dept_id ORDER BY hire_date DESC) AS rn
    FROM employees
)
SELECT *
FROM numbered
WHERE rn <= 1
ORDER BY hire_date, id;
