SELECT
    id,
    budget,
    location,
    INITCAP(TRIM(dept_name)) AS dept_name_title
FROM departments
ORDER BY location DESC, id DESC;
