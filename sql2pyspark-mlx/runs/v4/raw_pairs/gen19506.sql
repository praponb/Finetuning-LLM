WITH top_departments AS (
    SELECT
        id,
        location,
        budget,
        dept_name,
        DENSE_RANK() OVER (PARTITION BY location ORDER BY budget DESC) rn
    FROM departments
)
SELECT *
FROM top_departments
WHERE rn <= 3
ORDER BY budget;
