WITH numbered AS (
    SELECT
        id,
        location,
        budget,
        RANK() OVER (PARTITION BY location ORDER BY budget) rnk
    FROM departments
)
SELECT id, location, budget
FROM numbered
WHERE rnk <= 5
ORDER BY budget;
