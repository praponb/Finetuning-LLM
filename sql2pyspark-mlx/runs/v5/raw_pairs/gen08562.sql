SELECT
    id,
    budget,
    dept_name,
    location
FROM departments
WHERE budget <= (SELECT MIN(budget) FROM departments WHERE location IN ('London', 'Chiang Mai'))
ORDER BY dept_name
