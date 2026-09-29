WITH ranked_departments AS (
    SELECT
        id,
        location,
        budget,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS pos
    FROM departments
)
SELECT id, location, budget
FROM ranked_departments
WHERE pos <= 5;
