SELECT
    id,
    dept_name,
    budget,
    location
FROM departments
WHERE budget >= 2534000
ORDER BY location DESC;
