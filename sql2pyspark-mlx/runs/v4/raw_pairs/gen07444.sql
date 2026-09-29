SELECT id, dept_name, budget
FROM (
    SELECT
        id,
        dept_name,
        budget,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS rn
    FROM departments
    WHERE location IN ('Tokyo', 'Chiang Mai')
) AS ranked
WHERE rn <= 5
