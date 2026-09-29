SELECT
    id,
    budget,
    location,
    dept_name
FROM departments
WHERE budget < (SELECT MIN(budget) FROM departments);
