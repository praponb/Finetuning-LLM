SELECT id, location, budget
FROM (
    SELECT
        id,
        location,
        budget,
        DENSE_RANK() OVER (PARTITION BY location ORDER BY budget DESC) AS pos
    FROM departments
    WHERE location = 'Tokyo'
) AS t
WHERE pos = 1
ORDER BY budget ASC;
