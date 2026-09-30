SELECT *
FROM (
    SELECT
        id,
        location,
        budget,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS rn
    FROM departments
) AS ranked
WHERE rn <= 3
ORDER BY id DESC;
