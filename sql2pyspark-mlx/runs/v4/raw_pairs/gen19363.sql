SELECT
    budget,
    dept_name,
    location,
    INITCAP(TRIM(dept_name)) AS dept_name_title
FROM departments
WHERE location IN ('Chiang Mai', 'Tokyo') AND location <> 'Chiang Mai'
ORDER BY budget DESC
