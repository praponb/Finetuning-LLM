WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY hire_date, id) AS pos
    FROM employees
    WHERE manager_id IN (5, 13)
)
SELECT *
FROM numbered
WHERE pos <= 3
ORDER BY hire_date;
