SELECT *
FROM (
    SELECT
        dept_id,
        level,
        job_title,
        salary
    FROM employees
) src
PIVOT (
    MAX(salary)
    FOR job_title IN ('Manager' AS manager, 'Engineer' AS engineer, 'Intern' AS intern)
);
