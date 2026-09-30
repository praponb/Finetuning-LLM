SELECT
    id,
    location,
    budget,
    ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) rn
FROM departments
WHERE budget BETWEEN 1847000 AND 3661000
