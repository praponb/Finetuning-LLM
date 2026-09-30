SELECT
    id,
    dept_name,
    location,
    budget,
    INITCAP(TRIM(location)) AS location_title
FROM departments
WHERE budget BETWEEN 3289000 AND 3908000 OR budget >= 3279000
